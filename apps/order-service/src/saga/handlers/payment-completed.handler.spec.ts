import { Test, type TestingModule } from '@nestjs/testing';

import { KafkaEventProducer, LoggerService, MetricsService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import { PAYMENT_COMPLETED, type PaymentCompletedEvent } from '@surgepay/events';

import { OrderInboxRepository } from '../../repositories/inbox.repository';
import { SagaService } from '../saga.service';
import { SagaPaymentCompletedConsumer } from './payment-completed.handler';

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
        consumer: jest.fn().mockImplementation(() => {
          return {
            connect: jest.fn().mockResolvedValue(undefined),
            subscribe: jest.fn().mockResolvedValue(undefined),
            run: jest.fn().mockResolvedValue(undefined),
            disconnect: jest.fn().mockResolvedValue(undefined),
          };
        }),
      };
    }),
  };
});

describe('SagaPaymentCompletedConsumer', () => {
  let consumer: SagaPaymentCompletedConsumer;
  let sagaServiceMock: jest.Mocked<Partial<SagaService>>;
  let inboxRepoMock: jest.Mocked<Partial<OrderInboxRepository>>;
  let configMock: Partial<ConfigService>;
  let loggerMock: jest.Mocked<Partial<LoggerService>>;
  let producerMock: jest.Mocked<Partial<KafkaEventProducer>>;
  let metricsMock: jest.Mocked<Partial<MetricsService>>;

  beforeEach(async () => {
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

  it('should initialize with correct topic and derived groupId', () => {
    expect(consumer['topic']).toBe('payments.completed');
    expect(consumer['groupId']).toBe('test-order-group-saga');
  });

  it('should delegate to SagaService when handling a PaymentCompleted event', async () => {
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
    expect(loggerMock.info).toHaveBeenCalledWith(
      expect.stringContaining('Processing PaymentCompleted event inside Saga Orchestrator boundary'),
      expect.objectContaining({
        eventId: 'evt_99887',
        paymentId: 'pay_xyz',
        correlationId: 'corr_abcdef',
      }),
    );
  });
});
