import { Test, type TestingModule } from '@nestjs/testing';
import { KafkaEventProducer } from '@surgepay/common';
import { RelayModule } from '../../../apps/outbox-relay/src/relay.module';
import { OutboxPoller } from '../../../apps/outbox-relay/src/poller';
import { PrismaService } from '../../../apps/outbox-relay/src/prisma/prisma.service';
import { OutboxStatus } from '../../../apps/outbox-relay/src/generated/client';

describe('Outbox Relay Postgres SKIP LOCKED Integration', () => {
  let moduleFixture: TestingModule;
  let poller: OutboxPoller;
  let prismaService: PrismaService;

  beforeAll(async () => {
    // Modify DATABASE_URL in process.env to not contain ?schema parameter if needed,
    // because getOrCreatePrismaClient handles the schema name.
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
        publish: jest.fn().mockResolvedValue(undefined),
      })
      .compile();

    poller = moduleFixture.get<OutboxPoller>(OutboxPoller);
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
    // Clear outbox event records
    await prismaService.client.$executeRawUnsafe('TRUNCATE TABLE "payment"."OutboxEvent" CASCADE;');
  });

  it('multiple relay transactions using FOR UPDATE SKIP LOCKED must not acquire the same currently locked rows', async () => {
    // 1. Insert two pending events
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

    // 2. Start Transaction A which selects and locks the first event (batchSize = 1)
    let resolveTxA: () => void = () => {};
    const txAPromise = new Promise<void>((resolve) => {
      resolveTxA = resolve;
    });

    let polledEventsA: any[] = [];
    const txAExecution = prismaService.client.$transaction(async (tx) => {
      // Execute the lock raw query directly
      const sql = `
        SELECT id, "aggregateId", "aggregateType", "eventType", payload, status, "requestId", "correlationId", "causationId", "createdAt", "publishedAt", "retryCount"
        FROM "payment"."OutboxEvent"
        WHERE status = 'PENDING'
        ORDER BY "createdAt" ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      `;
      polledEventsA = await tx.$queryRawUnsafe<any[]>(sql);
      
      // Keep transaction open by waiting on the promise
      await txAPromise;
    });

    // Wait a brief moment to ensure Transaction A has acquired the lock
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Verify Transaction A locked the first event
    expect(polledEventsA).toHaveLength(1);
    expect(polledEventsA[0]?.id).toBe(event1.id);

    // 3. Start Transaction B (concurrently) and poll with batchSize = 1.
    // Transaction B should skip the locked event1 and acquire event2.
    const polledEventsB = await poller.pollPending(1);

    expect(polledEventsB).toHaveLength(1);
    expect(polledEventsB[0]?.id).toBe(event2.id);

    // 4. Resolve Transaction A, releasing its lock
    resolveTxA();
    await txAExecution;

    // 5. Verify that after Transaction A commits and releases lock,
    // another polling transaction can now acquire event1 (since its status is still PENDING).
    const polledEventsC = await poller.pollPending(1);
    expect(polledEventsC).toHaveLength(1);
    expect(polledEventsC[0]?.id).toBe(event1.id);
  });
});
