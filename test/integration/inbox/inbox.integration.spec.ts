import { randomUUID } from 'crypto';
import { Test, TestingModule } from '@nestjs/testing';
import { ConfigModule } from '@surgepay/config';
import { LoggerModule } from '@surgepay/common';
import { OrderModule } from '../../../apps/order-service/src/modules/order.module';
import { PrismaModule } from '../../../apps/order-service/src/prisma/prisma.module';
import { PrismaService } from '../../../apps/order-service/src/prisma/prisma.service';
import { OrderEventConsumer } from '../../../apps/order-service/src/services/order-event.consumer';
import { OrderInboxRepository } from '../../../apps/order-service/src/repositories/inbox.repository';
import { BaseEventEnvelope } from '@surgepay/events';
import { PAYMENT_INITIATED } from '@surgepay/events';
import { CURRENT_EVENT_VERSION } from '@surgepay/common';

// Mock kafkajs
const mockCommitOffsets = jest.fn().mockResolvedValue(undefined);
jest.mock('kafkajs', () => {
  return {
    Kafka: jest.fn().mockImplementation(() => {
      return {
        consumer: jest.fn().mockImplementation(() => {
          return {
            connect: jest.fn().mockResolvedValue(undefined),
            subscribe: jest.fn().mockResolvedValue(undefined),
            run: jest.fn().mockImplementation(async (options) => {
              (global as any).mockEachMessage = options.eachMessage;
            }),
            disconnect: jest.fn().mockResolvedValue(undefined),
            commitOffsets: mockCommitOffsets,
          };
        }),
      };
    }),
  };
});

describe('Inbox Pattern Idempotency Integration Tests', () => {
  let moduleFixture: TestingModule;
  let prismaService: PrismaService;
  let consumer: OrderEventConsumer;
  let repository: OrderInboxRepository;

  beforeAll(async () => {
    const originalUrl = process.env.DATABASE_URL;
    if (originalUrl) {
      const url = new URL(originalUrl);
      url.searchParams.delete('schema');
      process.env.DATABASE_URL = url.toString();
    }

    try {
      moduleFixture = await Test.createTestingModule({
        imports: [ConfigModule, LoggerModule, PrismaModule, OrderModule],
      }).compile();
    } finally {
      process.env.DATABASE_URL = originalUrl;
    }

    prismaService = moduleFixture.get<PrismaService>(PrismaService);
    consumer = moduleFixture.get<OrderEventConsumer>(OrderEventConsumer);
    repository = moduleFixture.get<OrderInboxRepository>(OrderInboxRepository);

    await prismaService.client.$connect();
    await consumer.onModuleInit();
  });

  afterAll(async () => {
    await consumer.onModuleDestroy();
    await prismaService.client.$disconnect();
    await moduleFixture.close();
  });

  beforeEach(async () => {
    jest.clearAllMocks();
    await prismaService.client.inboxEvent.deleteMany();
  });

  const createValidEnvelope = (eventId: string): BaseEventEnvelope<any> => {
    return {
      eventId,
      eventType: PAYMENT_INITIATED,
      correlationId: randomUUID(),
      causationId: randomUUID(),
      sagaId: randomUUID(),
      timestamp: new Date().toISOString(),
      version: CURRENT_EVENT_VERSION,
      payload: {
        paymentId: randomUUID(),
        amount: 5000,
        currency: 'USD',
        merchantId: randomUUID(),
        orderId: randomUUID(),
        paymentMethod: 'CREDIT_CARD',
      },
    };
  };

  it('Test Case 1: Event Deserialization and Validation - Rejects malformed envelopes', async () => {
    const malformedPayload = {
      eventId: '',
      eventType: PAYMENT_INITIATED,
      correlationId: '',
      causationId: randomUUID(),
      sagaId: randomUUID(),
      timestamp: 'invalid-date',
      version: CURRENT_EVENT_VERSION,
      payload: {},
    };

    const handler = (global as any).mockEachMessage;
    expect(handler).toBeDefined();

    await expect(
      handler({
        topic: 'payments.initiated',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(malformedPayload)),
          offset: '1',
        },
      }),
    ).rejects.toThrow();

    const count = await prismaService.client.inboxEvent.count();
    expect(count).toBe(0);
  });

  it('Test Case 2: First Delivery - Persists valid envelope as PROCESSED, executes handler and commits offset', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);
    const handleEventSpy = jest.spyOn(consumer as any, 'handleEvent').mockResolvedValue(undefined);

    const handler = (global as any).mockEachMessage;
    await handler({
      topic: 'payments.initiated',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '2',
      },
    });

    // Verify handler executed once
    expect(handleEventSpy).toHaveBeenCalledTimes(1);
    expect(handleEventSpy).toHaveBeenCalledWith(envelope);

    // Verify it is persisted in the database with PROCESSED status
    const persisted = await repository.findByEventIdAndConsumer(eventId, consumer['groupId']);
    expect(persisted).toBeDefined();
    expect(persisted?.eventId).toBe(eventId);
    expect(persisted?.status).toBe('PROCESSED');
    expect(persisted?.processedAt).toBeInstanceOf(Date);

    // Verify offset is committed (offset + 1)
    expect(mockCommitOffsets).toHaveBeenCalledTimes(1);
    expect(mockCommitOffsets).toHaveBeenCalledWith([
      { topic: 'payments.initiated', partition: 0, offset: '3' },
    ]);
  });

  it('Test Case 3: Duplicate Delivery - Skips handler and commits offset', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);
    const handleEventSpy = jest.spyOn(consumer as any, 'handleEvent').mockResolvedValue(undefined);

    const handler = (global as any).mockEachMessage;

    // First delivery
    await handler({
      topic: 'payments.initiated',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '10',
      },
    });

    expect(handleEventSpy).toHaveBeenCalledTimes(1);
    expect(mockCommitOffsets).toHaveBeenCalledTimes(1);

    mockCommitOffsets.mockClear();

    // Second delivery (Duplicate)
    await handler({
      topic: 'payments.initiated',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '12',
      },
    });

    // Handler should still only have been called once overall
    expect(handleEventSpy).toHaveBeenCalledTimes(1);
    
    // Offset should still be committed for the duplicate to acknowledge receipt
    expect(mockCommitOffsets).toHaveBeenCalledTimes(1);
    expect(mockCommitOffsets).toHaveBeenLastCalledWith([
      { topic: 'payments.initiated', partition: 0, offset: '13' },
    ]);
  });

  it('Test Case 4: PROCESSING In-Flight Blocks Commits', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);
    const handleEventSpy = jest.spyOn(consumer as any, 'handleEvent').mockResolvedValue(undefined);

    // Manually insert an event in PROCESSING status
    await prismaService.client.inboxEvent.create({
      data: {
        eventId,
        consumer: consumer['groupId'],
        status: 'PROCESSING',
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        causationId: envelope.causationId,
        sagaId: envelope.sagaId,
        timestamp: new Date(envelope.timestamp),
        version: envelope.version,
        payload: envelope.payload,
      },
    });

    const handler = (global as any).mockEachMessage;

    // Delivery should throw EventProcessingInProgressException
    await expect(
      handler({
        topic: 'payments.initiated',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(envelope)),
          offset: '20',
        },
      }),
    ).rejects.toThrow('is currently being processed by consumer');

    // Handler must NOT execute
    expect(handleEventSpy).not.toHaveBeenCalled();
    // Offset must NOT commit
    expect(mockCommitOffsets).not.toHaveBeenCalled();
  });

  it('Test Case 5: Interrupted Run Recovery - Executes handler for RETRYING status', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);
    const handleEventSpy = jest.spyOn(consumer as any, 'handleEvent').mockResolvedValue(undefined);

    // Manually insert in RETRYING status
    await prismaService.client.inboxEvent.create({
      data: {
        eventId,
        consumer: consumer['groupId'],
        status: 'RETRYING',
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        causationId: envelope.causationId,
        sagaId: envelope.sagaId,
        timestamp: new Date(envelope.timestamp),
        version: envelope.version,
        payload: envelope.payload,
      },
    });

    const handler = (global as any).mockEachMessage;
    await handler({
      topic: 'payments.initiated',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '30',
      },
    });

    // Handler executes
    expect(handleEventSpy).toHaveBeenCalledTimes(1);

    // Status transitions to PROCESSED
    const persisted = await repository.findByEventIdAndConsumer(eventId, consumer['groupId']);
    expect(persisted?.status).toBe('PROCESSED');

    // Offset committed
    expect(mockCommitOffsets).toHaveBeenCalledTimes(1);
    expect(mockCommitOffsets).toHaveBeenCalledWith([
      { topic: 'payments.initiated', partition: 0, offset: '31' },
    ]);
  });

  it('Test Case 6: Same Event, Different Consumer - Processes independently', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);

    // Persist as PROCESSED for the primary consumer
    await prismaService.client.inboxEvent.create({
      data: {
        eventId,
        consumer: consumer['groupId'],
        status: 'PROCESSED',
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        causationId: envelope.causationId,
        sagaId: envelope.sagaId,
        timestamp: new Date(envelope.timestamp),
        version: envelope.version,
        payload: envelope.payload,
      },
    });

    // Try to recordReceived for a different consumer - should succeed and not throw
    const otherConsumer = 'balance-service-consumer';
    const persisted = await repository.recordReceived(envelope, otherConsumer);
    expect(persisted).toBeDefined();
    expect(persisted.consumer).toBe(otherConsumer);
    expect(persisted.eventId).toBe(eventId);

    const count = await prismaService.client.inboxEvent.count();
    expect(count).toBe(2);
  });

  it('Test Case 7: Concurrent Duplicate Delivery - Single worker executes handler', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);

    // Mock handler with a small delay so that both workers overlap during execution
    const handleEventSpy = jest.spyOn(consumer as any, 'handleEvent').mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
    });

    const handler = (global as any).mockEachMessage;

    // Call two handlers in parallel
    const results = await Promise.allSettled([
      handler({
        topic: 'payments.initiated',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(envelope)),
          offset: '40',
        },
      }),
      handler({
        topic: 'payments.initiated',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(envelope)),
          offset: '41',
        },
      }),
    ]);

    // One worker must succeed, the other must throw EventProcessingInProgressException
    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');

    expect(fulfilled.length).toBe(1);
    expect(rejected.length).toBe(1);

    const error = (rejected[0] as PromiseRejectedResult).reason;
    expect(error.name).toBe('EventProcessingInProgressException');

    // Handler must only be executed once
    expect(handleEventSpy).toHaveBeenCalledTimes(1);

    // Database record should exist with the expected state
    const persisted = await repository.findByEventIdAndConsumer(eventId, consumer['groupId']);
    expect(persisted).toBeDefined();
  });

  it('Test Case 8: Handler Failure - Reverts status to RETRYING and increments retryCount', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);
    const handleEventSpy = jest.spyOn(consumer as any, 'handleEvent').mockRejectedValue(new Error('Business logic failed'));

    const handler = (global as any).mockEachMessage;

    await expect(
      handler({
        topic: 'payments.initiated',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(envelope)),
          offset: '50',
        },
      }),
    ).rejects.toThrow('Business logic failed');

    // Verify it was transitioned to RETRYING status with retryCount incremented to 1
    const persisted = await repository.findByEventIdAndConsumer(eventId, consumer['groupId']);
    expect(persisted?.status).toBe('RETRYING');
    expect(persisted?.retryCount).toBe(1);

    // Offset must NOT be committed
    expect(mockCommitOffsets).not.toHaveBeenCalled();
  });
});
