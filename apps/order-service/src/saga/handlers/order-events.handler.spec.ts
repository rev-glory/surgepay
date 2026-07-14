import { ConflictException } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';

import {
  KafkaEventProducer,
  LoggerService,
  MalformedEventEnvelopeException,
  MetricsService,
} from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import {
  ORDER_ELIGIBILITY_CONFIRMED,
  ORDER_ELIGIBILITY_REJECTED,
  type OrderEligibilityConfirmedEvent,
  type OrderEligibilityRejectedEvent,
  OrderEligibilityRejectedReason,
} from '@surgepay/events';

import {
  type InboxEvent,
  OrderValidationStatus,
  SagaStatus,
  SagaTransitionType,
} from '../../generated/client';
import { OrderInboxRepository } from '../../repositories/inbox.repository';
import { CommandDispatcher } from '../dispatchers/command.dispatcher';
import { SagaInstanceEntity } from '../entities/saga-instance.entity';
import { SagaRepository } from '../repositories/saga.repository';
import { SagaService } from '../saga.service';
import { SagaOrderEventsConsumer } from './order-events.handler';

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

describe('SagaOrderEventsConsumer & Saga Invariants', () => {
  let consumer: SagaOrderEventsConsumer;
  let sagaRepositoryMock: {
    findById: jest.Mock;
    findByPaymentId: jest.Mock;
    create: jest.Mock;
    update: jest.Mock;
  };
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

    sagaRepositoryMock = {
      findById: jest.fn(),
      findByPaymentId: jest.fn(),
      create: jest.fn(),
      update: jest.fn(),
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
        SagaOrderEventsConsumer,
        SagaService,
        { provide: SagaRepository, useValue: sagaRepositoryMock },
        { provide: OrderInboxRepository, useValue: inboxRepoMock },
        { provide: ConfigService, useValue: configMock },
        { provide: LoggerService, useValue: loggerMock },
        { provide: KafkaEventProducer, useValue: producerMock },
        { provide: MetricsService, useValue: metricsMock },
        {
          provide: CommandDispatcher,
          useValue: { dispatch: jest.fn() },
        },
      ],
    }).compile();

    consumer = module.get<SagaOrderEventsConsumer>(SagaOrderEventsConsumer);
  });

  afterEach(async () => {
    if (consumer) {
      await consumer.onModuleDestroy();
    }
  });

  describe('SagaInstanceEntity State Machine Invariant Tests', () => {
    it('1. REVERSED -> CLOSED remains valid', () => {
      const entity = new SagaInstanceEntity(
        'corr_123',
        'pay_123',
        'corr_123',
        SagaStatus.REVERSED,
        OrderValidationStatus.PENDING,
        'merch_xyz',
        5000,
        'USD',
        0,
        new Date(),
        null,
        new Date(),
        new Date()
      );
      entity.transitionTo(SagaStatus.CLOSED);
      expect(entity.status).toBe(SagaStatus.CLOSED);
      expect(entity.completedAt).toBeInstanceOf(Date);
    });

    it('2. REJECTED order validation blocks LEDGER_PENDING -> LEDGER_RECORDED', () => {
      const entity = new SagaInstanceEntity(
        'corr_123',
        'pay_123',
        'corr_123',
        SagaStatus.LEDGER_PENDING,
        OrderValidationStatus.REJECTED,
        'merch_xyz',
        5000,
        'USD',
        0,
        new Date(),
        null,
        new Date(),
        new Date()
      );
      expect(() => entity.transitionTo(SagaStatus.LEDGER_RECORDED)).toThrow(
        'Cannot transition financial status to LEDGER_RECORDED'
      );
    });

    it('3. CONFIRMED order validation allows LEDGER_PENDING -> LEDGER_RECORDED', () => {
      const entity = new SagaInstanceEntity(
        'corr_123',
        'pay_123',
        'corr_123',
        SagaStatus.LEDGER_PENDING,
        OrderValidationStatus.CONFIRMED,
        'merch_xyz',
        5000,
        'USD',
        0,
        new Date(),
        null,
        new Date(),
        new Date()
      );
      entity.transitionTo(SagaStatus.LEDGER_RECORDED);
      expect(entity.status).toBe(SagaStatus.LEDGER_RECORDED);
    });

    it('4. CONFIRMED -> REJECTED is rejected', () => {
      const entity = new SagaInstanceEntity(
        'corr_123',
        'pay_123',
        'corr_123',
        SagaStatus.LEDGER_PENDING,
        OrderValidationStatus.CONFIRMED,
        'merch_xyz',
        5000,
        'USD',
        0,
        new Date(),
        null,
        new Date(),
        new Date()
      );
      expect(() => entity.rejectOrder('Too late', 'order-service')).toThrow(
        'Invalid order validation transition'
      );
    });

    it('5. REJECTED -> CONFIRMED is rejected', () => {
      const entity = new SagaInstanceEntity(
        'corr_123',
        'pay_123',
        'corr_123',
        SagaStatus.LEDGER_PENDING,
        OrderValidationStatus.REJECTED,
        'merch_xyz',
        5000,
        'USD',
        0,
        new Date(),
        null,
        new Date(),
        new Date()
      );
      expect(() => entity.confirmOrder()).toThrow(
        'Invalid order validation transition from REJECTED to CONFIRMED'
      );
    });

    it('6. CLOSED rejects all further SagaStatus transitions', () => {
      const entity = new SagaInstanceEntity(
        'corr_123',
        'pay_123',
        'corr_123',
        SagaStatus.CLOSED,
        OrderValidationStatus.CONFIRMED,
        'merch_xyz',
        5000,
        'USD',
        0,
        new Date(),
        new Date(),
        new Date(),
        new Date()
      );
      expect(() => entity.transitionTo(SagaStatus.LEDGER_PENDING)).toThrow(
        'Cannot transition from terminal state'
      );
    });
  });

  describe('Consumer Event Processing and Service Delegation', () => {
    it('should process OrderEligibilityConfirmed event successfully', async () => {
      const event: OrderEligibilityConfirmedEvent = {
        eventId: 'evt_confirmed_001',
        eventType: ORDER_ELIGIBILITY_CONFIRMED,
        correlationId: 'corr_xyz',
        causationId: 'cause_001',
        sagaId: 'corr_xyz',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          orderId: 'ord_999',
        },
      };

      const saga = SagaInstanceEntity.create({
        paymentId: 'pay_xyz',
        correlationId: 'corr_xyz',
        merchantId: 'merch_xyz',
        amount: 5000,
        currency: 'USD',
      });

      sagaRepositoryMock.findById.mockResolvedValue(saga);
      sagaRepositoryMock.update.mockResolvedValue(saga);

      await consumer['handleEvent'](event);

      expect(sagaRepositoryMock.findById).toHaveBeenCalledWith('corr_xyz');
      expect(saga.orderValidationStatus).toBe(OrderValidationStatus.CONFIRMED);
      expect(sagaRepositoryMock.update).toHaveBeenCalledWith(
        saga,
        expect.arrayContaining([
          expect.objectContaining({
            transitionType: SagaTransitionType.ORDER_VALIDATION,
            fromState: OrderValidationStatus.PENDING,
            toState: OrderValidationStatus.CONFIRMED,
            eventId: 'evt_confirmed_001',
          }),
        ])
      );
    });

    it('should process OrderEligibilityRejected event successfully', async () => {
      const event: OrderEligibilityRejectedEvent = {
        eventId: 'evt_rejected_002',
        eventType: ORDER_ELIGIBILITY_REJECTED,
        correlationId: 'corr_xyz',
        causationId: 'cause_002',
        sagaId: 'corr_xyz',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          orderId: 'ord_999',
          reason: OrderEligibilityRejectedReason.AMOUNT_MISMATCH,
        },
      };

      const saga = SagaInstanceEntity.create({
        paymentId: 'pay_xyz',
        correlationId: 'corr_xyz',
        merchantId: 'merch_xyz',
        amount: 5000,
        currency: 'USD',
      });

      sagaRepositoryMock.findById.mockResolvedValue(saga);
      sagaRepositoryMock.update.mockResolvedValue(saga);

      await consumer['handleEvent'](event);

      expect(sagaRepositoryMock.findById).toHaveBeenCalledWith('corr_xyz');
      expect(saga.orderValidationStatus).toBe(OrderValidationStatus.REJECTED);
      expect(saga.failureReason).toContain('AMOUNT_MISMATCH');
      expect(saga.failedAt).toBeInstanceOf(Date);
      expect(saga.originService).toBe('order-service');

      expect(sagaRepositoryMock.update).toHaveBeenCalledWith(
        saga,
        expect.arrayContaining([
          expect.objectContaining({
            transitionType: SagaTransitionType.ORDER_VALIDATION,
            fromState: OrderValidationStatus.PENDING,
            toState: OrderValidationStatus.REJECTED,
            eventId: 'evt_rejected_002',
          }),
        ])
      );
    });

    it('should skip processing and log warning if saga is not found', async () => {
      const event: OrderEligibilityConfirmedEvent = {
        eventId: 'evt_confirmed_003',
        eventType: ORDER_ELIGIBILITY_CONFIRMED,
        correlationId: 'corr_not_found',
        causationId: 'cause_003',
        sagaId: 'corr_not_found',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          orderId: 'ord_none',
        },
      };

      sagaRepositoryMock.findById.mockResolvedValue(null);

      await consumer['handleEvent'](event);

      expect(sagaRepositoryMock.findById).toHaveBeenCalledWith('corr_not_found');
      expect(sagaRepositoryMock.update).not.toHaveBeenCalled();
    });

    it('should throw MalformedEventEnvelopeException for invalid payload', async () => {
      const event = {
        eventId: 'evt_bad_004',
        eventType: ORDER_ELIGIBILITY_CONFIRMED,
        correlationId: 'corr_xyz',
        causationId: 'cause_004',
        sagaId: 'corr_xyz',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {}, // missing orderId
      } as unknown as OrderEligibilityConfirmedEvent;

      await expect(consumer['handleEvent'](event)).rejects.toThrow(
        MalformedEventEnvelopeException
      );
    });

    it('should handle concurrency conflicts gracefully (optimistic locking check)', async () => {
      const event: OrderEligibilityConfirmedEvent = {
        eventId: 'evt_confirmed_005',
        eventType: ORDER_ELIGIBILITY_CONFIRMED,
        correlationId: 'corr_xyz',
        causationId: 'cause_005',
        sagaId: 'corr_xyz',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          orderId: 'ord_999',
        },
      };

      const saga = SagaInstanceEntity.create({
        paymentId: 'pay_xyz',
        correlationId: 'corr_xyz',
        merchantId: 'merch_xyz',
        amount: 5000,
        currency: 'USD',
      });

      sagaRepositoryMock.findById.mockResolvedValue(saga);
      sagaRepositoryMock.update.mockRejectedValue(
        new ConflictException('Optimistic locking failure')
      );

      await expect(consumer['handleEvent'](event)).rejects.toThrow(
        ConflictException
      );
    });

    it('should skip duplicate inbox messages and commit offset', async () => {
      const event: OrderEligibilityConfirmedEvent = {
        eventId: 'evt_dup_999',
        eventType: ORDER_ELIGIBILITY_CONFIRMED,
        correlationId: 'corr_xyz',
        causationId: 'cause_999',
        sagaId: 'corr_xyz',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          orderId: 'ord_999',
        },
      };

      inboxRepoMock.findByEventIdAndConsumer.mockResolvedValue({
        eventId: 'evt_dup_999',
        consumer: 'test-order-group-saga',
        status: 'PROCESSED',
      } as unknown as InboxEvent);

      await consumer.onModuleInit();

      expect(capturedEachMessage).toBeDefined();

      await capturedEachMessage!({
        topic: 'order.events',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(event)),
          offset: '500',
        },
      });

      expect(sagaRepositoryMock.findById).not.toHaveBeenCalled();
      expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
        {
          topic: 'order.events',
          partition: 0,
          offset: '501',
        },
      ]);
    });
  });
});
