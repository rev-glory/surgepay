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

describe('Inbox Pattern Foundation Integration Tests', () => {
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
      // Standard test environment uses the DATABASE_URL set by run-integration-tests.ts
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
    // Initialize consumer to set up Kafka subscription & eachMessage callback
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
      eventId: '', // Empty eventId is invalid
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

    // Verify nothing is persisted
    const count = await prismaService.client.inboxEvent.count();
    expect(count).toBe(0);
  });

  it('Test Case 2: Inbox Persistence - Persists valid PaymentInitiated envelope as RECEIVED', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);

    const handler = (global as any).mockEachMessage;
    await handler({
      topic: 'payments.initiated',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '2',
      },
    });

    // Verify it is persisted in the database
    const persisted = await repository.findByEventIdAndConsumer(eventId, consumer['groupId']);
    expect(persisted).toBeDefined();
    expect(persisted?.eventId).toBe(eventId);
    expect(persisted?.status).toBe('RECEIVED');
    expect(persisted?.eventType).toBe(PAYMENT_INITIATED);
    expect(persisted?.correlationId).toBe(envelope.correlationId);
    expect(persisted?.causationId).toBe(envelope.causationId);
    expect(persisted?.sagaId).toBe(envelope.sagaId);
    expect(persisted?.version).toBe(envelope.version);
    expect(persisted?.retryCount).toBe(0);
    expect(persisted?.processedAt).toBeNull();
    expect(persisted?.receivedAt).toBeInstanceOf(Date);
    expect(persisted?.timestamp).toBeInstanceOf(Date);
  });

  it('Test Case 3: DB Constraint Violation - Enforces unique consumer and eventId pair', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);

    const handler = (global as any).mockEachMessage;
    
    // First persist succeeds
    await handler({
      topic: 'payments.initiated',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '3',
      },
    });

    // Second persist with same eventId and consumer group throws unique constraint violation
    await expect(
      handler({
        topic: 'payments.initiated',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(envelope)),
          offset: '4',
        },
      }),
    ).rejects.toThrow();

    // Verify only 1 record exists
    const count = await prismaService.client.inboxEvent.count();
    expect(count).toBe(1);
  });

  it('Test Case 4: Manual Commit Capability - Available but not invoked on RECEIVED', async () => {
    const eventId = randomUUID();
    const envelope = createValidEnvelope(eventId);

    const handler = (global as any).mockEachMessage;

    // Successful receipt and database persistence
    await handler({
      topic: 'payments.initiated',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '5',
      },
    });

    // In Commit 5, offset commits must not be called upon receipt/persistence
    expect(mockCommitOffsets).not.toHaveBeenCalled();

    // Assert that the manual commit offsets function is available on the consumer class
    expect(consumer.commitOffset).toBeDefined();
    expect(typeof consumer.commitOffset).toBe('function');
  });
});
