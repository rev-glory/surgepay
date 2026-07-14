import { Test, type TestingModule } from '@nestjs/testing';

import {
  KafkaEventProducer,
  LoggerService,
  MalformedEventEnvelopeException,
  MetricsService,
} from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import {
  CHECK_ORDER_ELIGIBILITY,
  type CheckOrderEligibilityCommand,
  ORDER_ELIGIBILITY_CONFIRMED,
  ORDER_ELIGIBILITY_REJECTED,
  OrderEligibilityRejectedReason,
} from '@surgepay/events';

import { OrderEntity } from '../entities/order.entity';
import { type InboxEvent,OrderStatus } from '../generated/client';
import { OrderInboxRepository } from '../repositories/inbox.repository';
import { OrderService } from './order.service';
import { OrderEventConsumer } from './order-event.consumer';

// ---------------------------------------------------------------------------
// Kafka mock — prevents real socket connections
// ---------------------------------------------------------------------------

let capturedEachMessage:
  | ((options: {
      topic: string;
      partition: number;
      message: {
        value: Buffer | null;
        offset: string;
        headers?: Record<string, unknown>;
      };
    }) => Promise<void>)
  | null = null;

const mockConsumer = {
  connect: jest.fn().mockResolvedValue(undefined),
  subscribe: jest.fn().mockResolvedValue(undefined),
  run: jest.fn().mockImplementation((options: { eachMessage: unknown }) => {
    capturedEachMessage = options.eachMessage as typeof capturedEachMessage;
    return Promise.resolve();
  }),
  disconnect: jest.fn().mockResolvedValue(undefined),
  commitOffsets: jest.fn().mockResolvedValue(undefined),
};

jest.mock('kafkajs', () => ({
  CompressionTypes: { None: 0, GZIP: 1, Snappy: 2, LZ4: 3, ZSTD: 4 },
  Kafka: jest.fn().mockImplementation(() => ({
    producer: jest.fn().mockImplementation(() => ({
      connect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue([]),
      disconnect: jest.fn().mockResolvedValue(undefined),
    })),
    consumer: jest.fn().mockReturnValue(mockConsumer),
  })),
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Valid CheckOrderEligibility command envelope. */
const makeCommand = (overrides: Partial<CheckOrderEligibilityCommand> = {}): CheckOrderEligibilityCommand => ({
  eventId: 'cmd-uuid-111',
  eventType: CHECK_ORDER_ELIGIBILITY,
  correlationId: 'corr-uuid-aaa',
  causationId: 'cause-uuid-bbb',
  sagaId: 'saga-uuid-ccc',
  timestamp: new Date().toISOString(),
  version: 1,
  payload: {
    orderId: 'order-uuid-123',
    paymentId: 'payment-uuid-456',
    merchantId: 'merchant-uuid-789',
    amount: 10_000,
    currency: 'USD',
  },
  ...overrides,
});

/** Eligible order entity for mock returns. */
const eligibleOrder = new OrderEntity(
  'order-uuid-123',
  'merchant-uuid-789',
  10_000,
  'USD',
  OrderStatus.CREATED,
  'REF-001',
  new Date(),
  new Date(),
);

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe('OrderEventConsumer', () => {
  let consumer: OrderEventConsumer;
  let orderServiceMock: jest.Mocked<Pick<OrderService, 'validateOrderEligibilityById'>>;
  let inboxRepoMock: {
    findByEventIdAndConsumer: jest.Mock;
    recordReceived: jest.Mock;
    transitionStatus: jest.Mock;
    updateStatus: jest.Mock;
    countDlqDepth: jest.Mock;
  };
  let configMock: Partial<ConfigService>;
  let loggerMock: jest.Mocked<Partial<LoggerService>>;
  let producerMock: jest.Mocked<Partial<KafkaEventProducer>>;
  let metricsMock: jest.Mocked<Partial<MetricsService>>;

  beforeEach(async () => {
    capturedEachMessage = null;
    jest.clearAllMocks();

    orderServiceMock = {
      validateOrderEligibilityById: jest.fn(),
    };
    inboxRepoMock = {
      findByEventIdAndConsumer: jest.fn(),
      recordReceived: jest.fn(),
      transitionStatus: jest.fn(),
      updateStatus: jest.fn(),
      countDlqDepth: jest.fn().mockResolvedValue(0),
    };
    configMock = {
      kafka: {
        brokers: ['localhost:9092'],
        clientId: 'order-service-client',
        ssl: false,
        sasl: false,
        consumerGroupId: 'test-order-group',
        consumerRetryLimit: 3,
      },
      logging: {
        serviceName: 'order-service',
        level: 'info',
        pretty: false,
      },
    };
    loggerMock = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };
    producerMock = {
      publish: jest.fn().mockResolvedValue(undefined),
    };
    metricsMock = {
      setInboxDlqDepth: jest.fn(),
      recordConsumeAttempt: jest.fn(),
      recordInboxReceived: jest.fn(),
      recordInboxProcessed: jest.fn(),
      recordHandlerDuration: jest.fn(),
      recordHandlerFailure: jest.fn(),
      recordDuplicateSkip: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrderEventConsumer,
        { provide: OrderService, useValue: orderServiceMock },
        { provide: OrderInboxRepository, useValue: inboxRepoMock },
        { provide: ConfigService, useValue: configMock },
        { provide: LoggerService, useValue: loggerMock },
        { provide: KafkaEventProducer, useValue: producerMock },
        { provide: MetricsService, useValue: metricsMock },
      ],
    }).compile();

    consumer = module.get<OrderEventConsumer>(OrderEventConsumer);
  });

  afterEach(async () => {
    if (consumer) {
      await consumer.onModuleDestroy();
    }
  });

  // -------------------------------------------------------------------------
  // Test 1 — Topic and group ID
  // -------------------------------------------------------------------------
  it('should subscribe to order.commands with the correct group ID', () => {
    expect(consumer['topic']).toBe('order.commands');
    expect(consumer['groupId']).toBe('test-order-group-order-commands');
  });

  // -------------------------------------------------------------------------
  // Test 2 — Unsupported command types are skipped cleanly
  // -------------------------------------------------------------------------
  it('should log and skip unsupported command types without throwing', async () => {
    const unsupportedEnvelope = {
      eventId: 'evt-other-111',
      eventType: 'SomeOtherCommand',
      correlationId: 'corr-aaa',
      causationId: 'cause-bbb',
      sagaId: 'saga-ccc',
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {},
    };

    await consumer['handleEvent'](unsupportedEnvelope);

    expect(orderServiceMock.validateOrderEligibilityById).not.toHaveBeenCalled();
    expect(producerMock.publish).not.toHaveBeenCalled();
    expect(loggerMock.info).toHaveBeenCalledWith(
      expect.stringContaining('Unsupported command type'),
      expect.objectContaining({ eventType: 'SomeOtherCommand' }),
    );
  });

  // -------------------------------------------------------------------------
  // Test 3 — Malformed payload fields throw MalformedEventEnvelopeException
  // -------------------------------------------------------------------------
  it('should throw MalformedEventEnvelopeException for a CheckOrderEligibility command with missing payload fields', async () => {
    const malformed = makeCommand({
      payload: {
        orderId: '',          // empty — invalid
        paymentId: 'pay-456',
        merchantId: 'merch-789',
        amount: 10_000,
        currency: 'USD',
      },
    });

    await expect(consumer['handleEvent'](malformed)).rejects.toThrow(MalformedEventEnvelopeException);
    expect(orderServiceMock.validateOrderEligibilityById).not.toHaveBeenCalled();
    expect(producerMock.publish).not.toHaveBeenCalled();
  });

  // -------------------------------------------------------------------------
  // Test 4 — Eligible order → exactly one OrderEligibilityConfirmed published
  // -------------------------------------------------------------------------
  it('should publish exactly one OrderEligibilityConfirmed event for an eligible order', async () => {
    orderServiceMock.validateOrderEligibilityById.mockResolvedValue({
      eligible: true,
      order: eligibleOrder,
    });
    const command = makeCommand();

    await consumer['handleEvent'](command);

    expect(producerMock.publish).toHaveBeenCalledTimes(1);
    const [topic, key, event] = (producerMock.publish as jest.Mock).mock.calls[0];
    expect(topic).toBe('order.events');
    expect(key).toBe(command.sagaId);
    expect(event.eventType).toBe(ORDER_ELIGIBILITY_CONFIRMED);
    expect(event.payload.orderId).toBe(eligibleOrder.id);
  });

  // -------------------------------------------------------------------------
  // Test 5 — Ineligible order → exactly one OrderEligibilityRejected published
  // -------------------------------------------------------------------------
  it('should publish exactly one OrderEligibilityRejected event for an ineligible order', async () => {
    orderServiceMock.validateOrderEligibilityById.mockResolvedValue({
      eligible: false,
      reason: OrderEligibilityRejectedReason.ORDER_NOT_FOUND,
      orderId: null,
    });
    const command = makeCommand();

    await consumer['handleEvent'](command);

    expect(producerMock.publish).toHaveBeenCalledTimes(1);
    const [topic, key, event] = (producerMock.publish as jest.Mock).mock.calls[0];
    expect(topic).toBe('order.events');
    expect(key).toBe(command.sagaId);
    expect(event.eventType).toBe(ORDER_ELIGIBILITY_REJECTED);
    expect(event.payload.reason).toBe(OrderEligibilityRejectedReason.ORDER_NOT_FOUND);
    expect(event.payload.orderId).toBeNull();
  });

  // -------------------------------------------------------------------------
  // Test 6 — correlationId is preserved from command in result event
  // -------------------------------------------------------------------------
  it('should preserve correlationId from the incoming command in the result event', async () => {
    orderServiceMock.validateOrderEligibilityById.mockResolvedValue({
      eligible: true,
      order: eligibleOrder,
    });
    const command = makeCommand({ correlationId: 'specific-correlation-id' });

    await consumer['handleEvent'](command);

    const [, , event] = (producerMock.publish as jest.Mock).mock.calls[0];
    expect(event.correlationId).toBe('specific-correlation-id');
  });

  // -------------------------------------------------------------------------
  // Test 7 — sagaId is preserved from command in result event
  // -------------------------------------------------------------------------
  it('should preserve sagaId from the incoming command in the result event', async () => {
    orderServiceMock.validateOrderEligibilityById.mockResolvedValue({
      eligible: true,
      order: eligibleOrder,
    });
    const command = makeCommand({ sagaId: 'specific-saga-id' });

    await consumer['handleEvent'](command);

    const [, , event] = (producerMock.publish as jest.Mock).mock.calls[0];
    expect(event.sagaId).toBe('specific-saga-id');
  });

  // -------------------------------------------------------------------------
  // Test 8 — causationId of result event equals eventId of incoming command
  // -------------------------------------------------------------------------
  it('should set causationId of the result event to the eventId of the incoming command', async () => {
    orderServiceMock.validateOrderEligibilityById.mockResolvedValue({
      eligible: true,
      order: eligibleOrder,
    });
    const command = makeCommand({ eventId: 'original-command-event-id' });

    await consumer['handleEvent'](command);

    const [, , event] = (producerMock.publish as jest.Mock).mock.calls[0];
    expect(event.causationId).toBe('original-command-event-id');
  });

  // -------------------------------------------------------------------------
  // Test 9 — Result event has a new unique eventId (not reused from command)
  // -------------------------------------------------------------------------
  it('should generate a new unique eventId for the result event', async () => {
    orderServiceMock.validateOrderEligibilityById.mockResolvedValue({
      eligible: true,
      order: eligibleOrder,
    });
    const command = makeCommand({ eventId: 'cmd-uuid-111' });

    await consumer['handleEvent'](command);

    const [, , event] = (producerMock.publish as jest.Mock).mock.calls[0];
    expect(event.eventId).toBeDefined();
    expect(event.eventId).not.toBe('cmd-uuid-111');
    // Basic UUID v4 shape validation
    expect(event.eventId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
  });

  // -------------------------------------------------------------------------
  // Test 10 — OrderService DB failure causes handleEvent() to throw
  // -------------------------------------------------------------------------
  it('should propagate OrderService infrastructure failures so the Inbox is not marked PROCESSED', async () => {
    orderServiceMock.validateOrderEligibilityById.mockRejectedValue(
      new Error('Postgres connection lost'),
    );
    const command = makeCommand();

    await expect(consumer['handleEvent'](command)).rejects.toThrow('Postgres connection lost');

    // Publish must not have been called — no result event for a failed query
    expect(producerMock.publish).not.toHaveBeenCalled();
  });

  // -------------------------------------------------------------------------
  // Test 11 — KafkaEventProducer.publish failure causes handleEvent() to throw
  // -------------------------------------------------------------------------
  it('should propagate publish failures so the Inbox is not marked PROCESSED', async () => {
    orderServiceMock.validateOrderEligibilityById.mockResolvedValue({
      eligible: true,
      order: eligibleOrder,
    });
    (producerMock.publish as jest.Mock).mockRejectedValue(
      new Error('Kafka broker not available'),
    );
    const command = makeCommand();

    await expect(consumer['handleEvent'](command)).rejects.toThrow('Kafka broker not available');
  });

  // -------------------------------------------------------------------------
  // Test 12 — Full §9.1 envelope shape is correct on the confirmed event
  // -------------------------------------------------------------------------
  it('should emit a result event with a complete doc-v3 §9.1 envelope', async () => {
    orderServiceMock.validateOrderEligibilityById.mockResolvedValue({
      eligible: true,
      order: eligibleOrder,
    });
    const command = makeCommand();

    await consumer['handleEvent'](command);

    const [, , event] = (producerMock.publish as jest.Mock).mock.calls[0];
    // All §9.1 fields must be present and non-empty
    expect(typeof event.eventId).toBe('string');
    expect(event.eventId).toBeTruthy();
    expect(event.eventType).toBe(ORDER_ELIGIBILITY_CONFIRMED);
    expect(event.correlationId).toBe(command.correlationId);
    expect(event.causationId).toBe(command.eventId);
    expect(event.sagaId).toBe(command.sagaId);
    expect(typeof event.timestamp).toBe('string');
    expect(event.timestamp).toBeTruthy();
    expect(event.version).toBe(1);
    expect(event.payload).toBeDefined();
  });

  // -------------------------------------------------------------------------
  // Test 13 — Inbox Pattern: duplicate delivery is skipped and offset committed
  // -------------------------------------------------------------------------
  describe('Inbox Pattern integration', () => {
    it('should skip validation and not publish any event for a PROCESSED duplicate command', async () => {
      const command = makeCommand();

      // Simulate: Inbox already holds a PROCESSED record for this eventId
      inboxRepoMock.findByEventIdAndConsumer.mockResolvedValue({
        eventId: command.eventId,
        consumer: 'test-order-group-order-commands',
        status: 'PROCESSED',
        retryCount: 0,
      } as unknown as InboxEvent);

      await consumer.onModuleInit();
      expect(capturedEachMessage).toBeDefined();

      await capturedEachMessage!({
        topic: 'order.commands',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(command)),
          offset: '100',
          headers: {},
        },
      });

      // Neither business logic nor publication must have been triggered
      expect(orderServiceMock.validateOrderEligibilityById).not.toHaveBeenCalled();
      expect(producerMock.publish).not.toHaveBeenCalled();

      // Offset must be committed (processed + 1)
      expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
        { topic: 'order.commands', partition: 0, offset: '101' },
      ]);
      expect(metricsMock.recordDuplicateSkip).toHaveBeenCalled();
    });
  });
});
