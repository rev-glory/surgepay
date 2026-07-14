import { Test, type TestingModule } from '@nestjs/testing';
import { KafkaEventProducer } from '@surgepay/common';
import { RelayModule } from '../../../apps/outbox-relay/src/relay.module';
import { OutboxPoller } from '../../../apps/outbox-relay/src/poller';
import { PrismaService } from '../../../apps/outbox-relay/src/prisma/prisma.service';
import { OutboxStatus } from '../../../apps/outbox-relay/src/generated/client';
import { OutboxRepository } from '../../../apps/outbox-relay/src/repositories/outbox.repository';

describe('Outbox Relay Integration & Concurrency Tests', () => {
  let moduleFixture: TestingModule;
  let poller: OutboxPoller;
  let repository: OutboxRepository;
  let prismaService: PrismaService;

  beforeAll(async () => {
    const originalUrl = process.env.DATABASE_URL;
    if (originalUrl) {
      const url = new URL(originalUrl);
      url.searchParams.delete('schema');
      process.env.DATABASE_URL = url.toString();
    }

    moduleFixture = await Test.createTestingModule({
      imports: [RelayModule],
    })
      .overrideProvider(KafkaEventProducer)
      .useValue({
        onModuleInit: jest.fn().mockResolvedValue(undefined),
        onModuleDestroy: jest.fn().mockResolvedValue(undefined),
        publish: jest.fn().mockResolvedValue([
          { topicName: 'payments.initiated', partition: 0, offset: '0' },
        ]),
      })
      .compile();

    poller = moduleFixture.get<OutboxPoller>(OutboxPoller);
    repository = moduleFixture.get<OutboxRepository>(OutboxRepository);
    prismaService = moduleFixture.get<PrismaService>(PrismaService);

    await prismaService.client.$connect();
  });

  afterAll(async () => {
    if (prismaService) {
      await prismaService.client.$disconnect();
    }
    if (moduleFixture) {
      await moduleFixture.close();
    }
  });

  beforeEach(async () => {
    await prismaService.client.$executeRawUnsafe('TRUNCATE TABLE "payment"."OutboxEvent" CASCADE;');
  });

  it('multiple relay transactions using FOR UPDATE SKIP LOCKED must not acquire the same currently locked rows', async () => {
    const event1 = await prismaService.client.outboxEvent.create({
      data: {
        id: '11111111-1111-1111-1111-111111111111',
        aggregateId: '00000000-0000-0000-0000-000000000001',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: { amount: 1000 },
        status: OutboxStatus.PENDING,
        requestId: 'req_lock_1',
        correlationId: 'corr_lock_1',
        causationId: 'caus_lock_1',
      },
    });

    const event2 = await prismaService.client.outboxEvent.create({
      data: {
        id: '22222222-2222-2222-2222-222222222222',
        aggregateId: '00000000-0000-0000-0000-000000000002',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: { amount: 2000 },
        status: OutboxStatus.PENDING,
        requestId: 'req_lock_2',
        correlationId: 'corr_lock_2',
        causationId: 'caus_lock_2',
      },
    });

    let resolveTxA: () => void = () => {};
    const txAPromise = new Promise<void>((resolve) => {
      resolveTxA = resolve;
    });

    let polledEventsA: any[] = [];
    const txAExecution = prismaService.client.$transaction(async (tx) => {
      const sql = `
        SELECT id FROM "payment"."OutboxEvent"
        WHERE status = 'PENDING'
        ORDER BY "createdAt" ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      `;
      polledEventsA = await tx.$queryRawUnsafe<any[]>(sql);
      await txAPromise;
    });

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(polledEventsA).toHaveLength(1);
    expect(polledEventsA[0]?.id).toBe(event1.id);

    const polledEventsB = await poller.pollPending(1);

    expect(polledEventsB).toHaveLength(1);
    expect(polledEventsB[0]?.id).toBe(event2.id);

    resolveTxA();
    await txAExecution;

    const polledEventsC = await poller.pollPending(1);
    expect(polledEventsC).toHaveLength(1);
    expect(polledEventsC[0]?.id).toBe(event1.id);
  });

  it('recovers stale PUBLISHING events but ignores non-stale ones', async () => {
    const staleId = '33333333-3333-3333-3333-333333333333';
    const nonStaleId = '44444444-4444-4444-4444-444444444444';

    await prismaService.client.outboxEvent.create({
      data: {
        id: staleId,
        aggregateId: '00000000-0000-0000-0000-000000000003',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: { amount: 3000 },
        status: OutboxStatus.PUBLISHING,
        requestId: 'req_stale',
        correlationId: 'corr_stale',
        causationId: 'caus_stale',
        lastAttemptAt: new Date(Date.now() - 600000), // 10 minutes ago
      },
    });

    await prismaService.client.outboxEvent.create({
      data: {
        id: nonStaleId,
        aggregateId: '00000000-0000-0000-0000-000000000004',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: { amount: 4000 },
        status: OutboxStatus.PUBLISHING,
        requestId: 'req_nonstale',
        correlationId: 'corr_nonstale',
        causationId: 'caus_nonstale',
        lastAttemptAt: new Date(Date.now() - 10000), // 10 seconds ago
      },
    });

    // Run recovery with a 5-minute timeout (300,000 ms) and a retry limit of 3
    await poller.recoverStale(300000, 3);

    // Verify stale event is now RETRYING and retryCount incremented
    const staleDb = await prismaService.client.outboxEvent.findUnique({ where: { id: staleId } });
    expect(staleDb?.status).toBe(OutboxStatus.RETRYING);
    expect(staleDb?.retryCount).toBe(1);

    // Verify non-stale event remains PUBLISHING
    const nonStaleDb = await prismaService.client.outboxEvent.findUnique({ where: { id: nonStaleId } });
    expect(nonStaleDb?.status).toBe(OutboxStatus.PUBLISHING);
    expect(nonStaleDb?.retryCount).toBe(0);
  });

  it('enforces retry limit during recovery and transitions exhausted events to permanently FAILED', async () => {
    const exhaustedId = '55555555-5555-5555-5555-555555555555';

    await prismaService.client.outboxEvent.create({
      data: {
        id: exhaustedId,
        aggregateId: '00000000-0000-0000-0000-000000000005',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: { amount: 5000 },
        status: OutboxStatus.PUBLISHING,
        requestId: 'req_exhausted',
        correlationId: 'corr_exhausted',
        causationId: 'caus_exhausted',
        retryCount: 2, // next attempt will be 3 (exhausted)
        lastAttemptAt: new Date(Date.now() - 600000),
      },
    });

    await poller.recoverStale(300000, 3);

    const dbEvent = await prismaService.client.outboxEvent.findUnique({ where: { id: exhaustedId } });
    expect(dbEvent?.status).toBe(OutboxStatus.FAILED);
    expect(dbEvent?.retryCount).toBe(3);
  });

  it('keeps same Event ID across stale recovery cycles', async () => {
    const staleId = '66666666-6666-6666-6666-666666666666';

    await prismaService.client.outboxEvent.create({
      data: {
        id: staleId,
        aggregateId: '00000000-0000-0000-0000-000000000006',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: { amount: 6000 },
        status: OutboxStatus.PUBLISHING,
        requestId: 'req_same_id',
        correlationId: 'corr_same_id',
        causationId: 'caus_same_id',
        lastAttemptAt: new Date(Date.now() - 600000),
      },
    });

    await poller.recoverStale(300000, 3);
    const dbEvent = await prismaService.client.outboxEvent.findUnique({ where: { id: staleId } });
    expect(dbEvent?.id).toBe(staleId);
  });

  it('never reclaims PUBLISHED events during stale recovery or polling', async () => {
    const publishedId = '77777777-7777-7777-7777-777777777777';

    await prismaService.client.outboxEvent.create({
      data: {
        id: publishedId,
        aggregateId: '00000000-0000-0000-0000-000000000007',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: { amount: 7000 },
        status: OutboxStatus.PUBLISHED,
        requestId: 'req_published',
        correlationId: 'corr_published',
        causationId: 'caus_published',
        lastAttemptAt: new Date(Date.now() - 600000),
      },
    });

    await poller.recoverStale(300000, 3);
    const staleRecoveryCheck = await prismaService.client.outboxEvent.findUnique({ where: { id: publishedId } });
    expect(staleRecoveryCheck?.status).toBe(OutboxStatus.PUBLISHED);

    const polledEvents = await poller.pollPending(10);
    expect(polledEvents).toHaveLength(0);
  });

  it('rejects invalid state transitions via repository validation constraints', async () => {
    const pendingId = '88888888-8888-8888-8888-888888888888';

    await prismaService.client.outboxEvent.create({
      data: {
        id: pendingId,
        aggregateId: '00000000-0000-0000-0000-000000000008',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: { amount: 8000 },
        status: OutboxStatus.PENDING,
        requestId: 'req_pending',
        correlationId: 'corr_pending',
        causationId: 'caus_pending',
      },
    });

    // markPublished must only work on events in PUBLISHING status.
    // Try marking it published while it is still PENDING -> should fail update.
    await expect(repository.markPublished(pendingId, 1, '100')).rejects.toThrow();

    // markFailed must only work on events in PUBLISHING status.
    await expect(repository.markFailed(pendingId, 'Mock error')).rejects.toThrow();
  });
});
