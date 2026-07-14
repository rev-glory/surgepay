import { Test, type TestingModule } from '@nestjs/testing';

import {
  KafkaEventProducer,
  LoggerService,
  MalformedEventEnvelopeException,
  MetricsService,
} from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import {
  PAYMENT_COMPLETED,
  PAYMENT_FAILED,
  PAYMENT_INITIATED,
  type PaymentCompletedEvent,
} from '@surgepay/events';

import { type InboxEvent } from '../../generated/client';
import { OrderInboxRepository } from '../../repositories/inbox.repository';
import { SagaService } from '../saga.service';
import { SagaPaymentCompletedConsumer } from './payment-completed.handler';

// Define the mock consumer outside so the mock factory can return it
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
    capturedEachMessage = options.eachMessage as typeof capturedEachMessage; // safe cast for inner framework assignment
    return Promise.resolve();
  }),
  disconnect: jest.fn().mockResolvedValue(undefined),
  commitOffsets: jest.fn().mockResolvedValue(undefined),
};

// Mock kafkajs to prevent real socket creation during base consumer class constructor initialization
jest.mock('kafkajs', () => {
  return {
    CompressionTypes: {
      None: 0,
      GZIP: 1,
      Snappy: 2,
      LZ4: 3,
      ZSTD: 4,
    },
    Kafka: jest.fn().mockImplementation(() => {
      return {
        producer: jest.fn().mockImplementation(() => {
          return {
            connect: jest.fn().mockResolvedValue(undefined),
            send: jest.fn().mockResolvedValue([]),
            disconnect: jest.fn().mockResolvedValue(undefined),
          };
        }),
        consumer: jest.fn().mockReturnValue(mockConsumer),
      };
    }),
  };
});

describe('SagaPaymentCompletedConsumer', () => {
  let consumer: SagaPaymentCompletedConsumer;
  let sagaServiceMock: jest.Mocked<Partial<SagaService>>;
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

    sagaServiceMock = {
      processPaymentCompleted: jest.fn().mockResolvedValue(undefined),
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
      publish: jest.fn(),
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
        SagaPaymentCompletedConsumer,
        { provide: SagaService, useValue: sagaServiceMock },
        { provide: OrderInboxRepository, useValue: inboxRepoMock },
        { provide: ConfigService, useValue: configMock },
        { provide: LoggerService, useValue: loggerMock },
        { provide: KafkaEventProducer, useValue: producerMock },
        { provide: MetricsService, useValue: metricsMock },
      ],
    }).compile();

    consumer = module.get<SagaPaymentCompletedConsumer>(SagaPaymentCompletedConsumer);
  });

  afterEach(async () => {
    if (consumer) {
      await consumer.onModuleDestroy();
    }
  });

  it('should initialize with correct topic and group ID', () => {
    expect(consumer['topic']).toBe('payments.completed');
    expect(consumer['groupId']).toBe('test-order-group-saga');
  });

  describe('event validation and filtering', () => {
    it('should delegate valid PaymentCompleted events to SagaService', async () => {
      const event: PaymentCompletedEvent = {
        eventId: 'evt_99887',
        eventType: PAYMENT_COMPLETED,
        correlationId: 'corr_abcdef',
        causationId: 'cause_112233',
        sagaId: 'saga_abcdef',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId: 'pay_xyz',
          amount: 3000,
          currency: 'EUR',
          merchantId: 'merch_456',
          orderId: 'ord_789',
          processorTransactionId: 'txn_processor_abc',
          completedAt: new Date().toISOString(),
        },
      };

      await consumer['handleEvent'](event);

      expect(sagaServiceMock.processPaymentCompleted).toHaveBeenCalledWith(event);
    });

    it('should filter and ignore recognized non-owned event types cleanly', async () => {
      const event = {
        eventId: 'evt_1111',
        eventType: PAYMENT_INITIATED,
        correlationId: 'corr_abcdef',
        causationId: 'cause_112233',
        sagaId: 'saga_abcdef',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {},
      } as unknown as PaymentCompletedEvent;

      await consumer['handleEvent'](event);

      expect(sagaServiceMock.processPaymentCompleted).not.toHaveBeenCalled();
      expect(loggerMock.info).toHaveBeenCalledWith(
        expect.stringContaining('Ignoring non-owned event type inside Saga Orchestrator consumer'),
        expect.objectContaining({ eventType: PAYMENT_INITIATED })
      );

      // Verify same filtering for PaymentFailed
      const failedEvent = {
        ...event,
        eventType: PAYMENT_FAILED,
      } as unknown as PaymentCompletedEvent;

      await consumer['handleEvent'](failedEvent);
      expect(sagaServiceMock.processPaymentCompleted).not.toHaveBeenCalled();
    });

    it('should reject PaymentCompleted events with missing payload fields', async () => {
      const event = {
        eventId: 'evt_2222',
        eventType: PAYMENT_COMPLETED,
        correlationId: 'corr_abcdef',
        causationId: 'cause_112233',
        sagaId: 'saga_abcdef',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId: '', // Invalid
          amount: 3000,
          currency: 'EUR',
          merchantId: 'merch_456',
        },
      } as unknown as PaymentCompletedEvent;

      await expect(consumer['handleEvent'](event)).rejects.toThrow(
        MalformedEventEnvelopeException
      );
    });
  });

  describe('Inbox Pattern integration', () => {
    it('should skip saga execution and commit offset for duplicate Inbox events', async () => {
      const event: PaymentCompletedEvent = {
        eventId: 'evt_dup_123',
        eventType: PAYMENT_COMPLETED,
        correlationId: 'corr_54321',
        causationId: 'cause_99999',
        sagaId: 'saga_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId: 'pay_abc',
          amount: 5000,
          currency: 'USD',
          merchantId: 'merch_xyz',
          orderId: 'ord_123',
          processorTransactionId: 'txn_processor',
          completedAt: new Date().toISOString(),
        },
      };

      // Mock database to indicate this message was already PROCESSED
      inboxRepoMock.findByEventIdAndConsumer.mockResolvedValue({
        eventId: 'evt_dup_123',
        consumer: 'test-order-group-saga',
        status: 'PROCESSED',
        retryCount: 0,
      } as unknown as InboxEvent);

      // Initialize module which starts consumer.run
      await consumer.onModuleInit();
      expect(capturedEachMessage).toBeDefined();

      // Trigger the eachMessage callback
      await capturedEachMessage!({
        topic: 'payments.completed',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(event)),
          offset: '200',
          headers: {},
        },
      });

      // Assert early exit: sagaService never called, offset committed
      expect(sagaServiceMock.processPaymentCompleted).not.toHaveBeenCalled();
      expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
        {
          topic: 'payments.completed',
          partition: 0,
          offset: '201',
        },
      ]);
      expect(metricsMock.recordDuplicateSkip).toHaveBeenCalled();
    });
  });
});
