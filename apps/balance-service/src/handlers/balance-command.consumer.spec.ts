/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { Test, type TestingModule } from '@nestjs/testing';

import {
  KafkaEventProducer,
  LoggerService,
  MalformedEventEnvelopeException,
  MetricsService,
} from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import {
  BALANCE_RESERVATION_FAILED,
  BALANCE_RESERVED,
  BALANCE_REVERSED,
  RESERVE_BALANCE,
  type ReserveBalanceCommand,
  REVERSE_BALANCE,
  type ReverseBalanceCommand,
} from '@surgepay/events';

import { MerchantBalanceEntity } from '../entities/merchant-balance.entity';
import { PrismaService } from '../prisma/prisma.service';
import { BalanceRepository } from '../repositories/balance.repository';
import { BalanceInboxRepository } from '../repositories/inbox.repository';
import { OutboxRepository } from '../repositories/outbox.repository';
import { BalanceService } from '../services/balance.service';
import { OutboxRelayWorker } from '../services/outbox-relay.worker';
import { BalanceCommandConsumer } from './balance-command.consumer';

// Mock kafkajs to prevent socket connection attempts
jest.mock('kafkajs', () => ({
  CompressionTypes: { None: 0, GZIP: 1, Snappy: 2, LZ4: 3, ZSTD: 4 },
  Kafka: jest.fn().mockImplementation(() => ({
    producer: jest.fn().mockImplementation(() => ({
      connect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue([]),
      disconnect: jest.fn().mockResolvedValue(undefined),
    })),
    consumer: jest.fn().mockImplementation(() => ({
      connect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
    })),
  })),
}));

describe('Balance Command Handling & Domain Logic Spec', () => {
  let consumer: BalanceCommandConsumer;
  let balanceService: BalanceService;
  let balanceRepository: BalanceRepository;
  let outboxRepository: OutboxRepository;
  let inboxRepository: BalanceInboxRepository;
  let prismaService: PrismaService;
  let outboxRelayWorker: OutboxRelayWorker;

  // Mocked dependencies
  const mockProducer = {
    publish: jest.fn().mockResolvedValue([{ topic: 'balance.events', partition: 0, offset: '1' }]),
  };

  const mockLogger = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    setContext: jest.fn(),
  };

  const mockConfig = {
    kafka: {
      consumerGroupId: 'test-balance-group',
    },
  };

  const mockMetrics = {
    setOutboxPending: jest.fn(),
    setOutboxFailed: jest.fn(),
    setOutboxPublished: jest.fn(),
    setOutboxInFlight: jest.fn(),
    recordOutboxLag: jest.fn(),
    recordOutboxBatchSize: jest.fn(),
    recordOutboxCycleDuration: jest.fn(),
    recordPublicationRetry: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        BalanceCommandConsumer,
        BalanceService,
        {
          provide: BalanceRepository,
          useValue: {
            create: jest.fn(),
            findByMerchantId: jest.fn(),
            findByMerchantIdAndCurrency: jest.fn(),
            reserveFunds: jest.fn(),
            releaseFunds: jest.fn(),
            findReversalByPaymentId: jest.fn(),
            createReversal: jest.fn(),
          },
        },
        {
          provide: OutboxRepository,
          useValue: {
            save: jest.fn(),
            findPending: jest.fn(),
            markPublished: jest.fn(),
            incrementRetry: jest.fn(),
            markFailed: jest.fn(),
          },
        },
        {
          provide: BalanceInboxRepository,
          useValue: {
            findByEventIdAndConsumer: jest.fn(),
            recordReceived: jest.fn(),
            transitionStatus: jest.fn(),
            updateStatus: jest.fn(),
          },
        },
        {
          provide: PrismaService,
          useValue: {
            client: {
              $transaction: jest.fn().mockImplementation(async (callback) => {
                return callback({});
              }),
            },
          },
        },
        OutboxRelayWorker,
        { provide: KafkaEventProducer, useValue: mockProducer },
        { provide: LoggerService, useValue: mockLogger },
        { provide: ConfigService, useValue: mockConfig },
        { provide: MetricsService, useValue: mockMetrics },
      ],
    }).compile();

    consumer = module.get<BalanceCommandConsumer>(BalanceCommandConsumer);
    balanceService = module.get<BalanceService>(BalanceService);
    balanceRepository = module.get<BalanceRepository>(BalanceRepository);
    outboxRepository = module.get<OutboxRepository>(OutboxRepository);
    inboxRepository = module.get<BalanceInboxRepository>(BalanceInboxRepository);
    prismaService = module.get<PrismaService>(PrismaService);
    outboxRelayWorker = module.get<OutboxRelayWorker>(OutboxRelayWorker);
  });

  const makeValidCommand = (): ReserveBalanceCommand => ({
    eventId: 'cmd_balance_123',
    eventType: RESERVE_BALANCE,
    correlationId: 'corr_xyz',
    causationId: 'cause_abc',
    sagaId: 'corr_xyz',
    timestamp: new Date().toISOString(),
    version: 1,
    payload: {
      paymentId: 'pay_111',
      merchantId: 'merch_222',
      amount: 15000,
      currency: 'USD',
    },
  });

  const makeValidReverseCommand = (): ReverseBalanceCommand => ({
    eventId: 'cmd_rev_bal_123',
    eventType: REVERSE_BALANCE,
    correlationId: 'corr_xyz',
    causationId: 'cause_abc',
    sagaId: 'corr_xyz',
    timestamp: new Date().toISOString(),
    version: 1,
    payload: {
      paymentId: 'pay_111',
      merchantId: 'merch_222',
      amount: 15000,
      currency: 'USD',
      reason: 'Saga compensation reversal',
    },
  });

  describe('BalanceCommandConsumer handleEvent', () => {
    it('should delegate valid ReserveBalance command to BalanceService', async () => {
      const command = makeValidCommand();
      jest.spyOn(balanceService, 'reserve').mockResolvedValue({
        success: true,
      });

      // Invoke the protected handler directly
      await (consumer as any).handleEvent(command);

      expect(balanceService.reserve).toHaveBeenCalledWith(command.payload, command);
      expect(mockLogger.info).toHaveBeenCalledWith(
        'Processing ReserveBalance command',
        expect.any(Object)
      );
    });

    it('should ignore unsupported event types cleanly', async () => {
      const command = { ...makeValidCommand(), eventType: 'UnsupportedType' };
      const reserveSpy = jest.spyOn(balanceService, 'reserve');
      await (consumer as any).handleEvent(command);

      expect(reserveSpy).not.toHaveBeenCalled();
      expect(mockLogger.info).toHaveBeenCalledWith(
        expect.stringContaining('Unsupported command type'),
        expect.any(Object)
      );
    });

    it('should throw MalformedEventEnvelopeException if payload is invalid', async () => {
      const command = makeValidCommand();
      command.payload.amount = undefined as any; // invalidate

      await expect((consumer as any).handleEvent(command)).rejects.toThrow(
        MalformedEventEnvelopeException
      );
    });

    it('should finish cleanly and not bubble up exception on business/validation failure', async () => {
      const command = makeValidCommand();
      jest.spyOn(balanceService, 'reserve').mockResolvedValue({
        success: false,
        reason: 'Insufficient available balance',
      });

      await (consumer as any).handleEvent(command);

      expect(balanceService.reserve).toHaveBeenCalledWith(command.payload, command);
      expect(mockLogger.error).toHaveBeenCalledWith(
        'ReserveBalance failed permanently',
        expect.any(Object)
      );
    });

    it('should delegate valid ReverseBalance command to BalanceService', async () => {
      const command = makeValidReverseCommand();
      jest.spyOn(balanceService, 'reverse').mockResolvedValue({
        success: true,
      });

      await (consumer as any).handleEvent(command);

      expect(balanceService.reverse).toHaveBeenCalledWith(command.payload, command);
      expect(mockLogger.info).toHaveBeenCalledWith(
        'Processing ReverseBalance command',
        expect.any(Object)
      );
    });

    it('should throw MalformedEventEnvelopeException if ReverseBalance payload is invalid', async () => {
      const command = makeValidReverseCommand();
      command.payload.reason = undefined as any;

      await expect((consumer as any).handleEvent(command)).rejects.toThrow(
        MalformedEventEnvelopeException
      );
    });

    it('should finish cleanly and not bubble up exception on ReverseBalance permanent failure', async () => {
      const command = makeValidReverseCommand();
      jest.spyOn(balanceService, 'reverse').mockResolvedValue({
        success: false,
        reason: 'Merchant balance not found',
      });

      await (consumer as any).handleEvent(command);

      expect(balanceService.reverse).toHaveBeenCalledWith(command.payload, command);
      expect(mockLogger.error).toHaveBeenCalledWith(
        'ReverseBalance failed permanently',
        expect.any(Object)
      );
    });
  });

  describe('BalanceService Reservation & Idempotency logic', () => {
    it('should decrease available balance, increase reserved, and write BalanceReserved outbox record with correct tracing', async () => {
      const command = makeValidCommand();
      const payload = command.payload;

      const mockBalance = new MerchantBalanceEntity(
        'balance_uuid',
        payload.merchantId,
        payload.currency,
        20000, // available
        0, // reserved
        new Date()
      );

      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([mockBalance]);
      jest.spyOn(balanceRepository, 'reserveFunds').mockResolvedValue(true);

      const result = await balanceService.reserve(payload, command);

      expect(result.success).toBe(true);
      expect(balanceRepository.reserveFunds).toHaveBeenCalledWith(
        payload.merchantId,
        payload.currency,
        payload.amount,
        expect.any(Object)
      );

      // Verify BalanceReserved outbox write
      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: BALANCE_RESERVED,
          correlationId: command.correlationId,
          causationId: command.eventId,
          payload: expect.objectContaining({
            payload: expect.objectContaining({
              paymentId: payload.paymentId,
              merchantId: payload.merchantId,
              amount: payload.amount,
              currency: payload.currency,
              reservationId: expect.any(String),
            }),
          }),
        }),
        expect.any(Object)
      );
    });

    it('should reject with BalanceReservationFailed on insufficient available balance and leave balance unchanged', async () => {
      const command = makeValidCommand();
      const payload = command.payload;

      const mockBalance = new MerchantBalanceEntity(
        'balance_uuid',
        payload.merchantId,
        payload.currency,
        5000, // available (insufficient for 15000)
        0, // reserved
        new Date()
      );

      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([mockBalance]);
      jest.spyOn(balanceRepository, 'reserveFunds').mockResolvedValue(false); // atomic failure

      const result = await balanceService.reserve(payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toBe('Insufficient available balance');
      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: BALANCE_RESERVATION_FAILED,
          correlationId: command.correlationId,
          causationId: command.eventId,
          payload: expect.objectContaining({
            payload: expect.objectContaining({
              reason: 'Insufficient available balance',
            }),
          }),
        }),
        expect.any(Object)
      );
    });

    it('should reject with BalanceReservationFailed on currency mismatch and leave balance unchanged', async () => {
      const command = makeValidCommand();
      const payload = command.payload;

      const mockBalanceEUR = new MerchantBalanceEntity(
        'balance_uuid_eur',
        payload.merchantId,
        'EUR', // mismatched currency
        20000,
        0,
        new Date()
      );

      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([mockBalanceEUR]);

      const result = await balanceService.reserve(payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toContain('Currency mismatch');
      expect(balanceRepository.reserveFunds).not.toHaveBeenCalled();

      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: BALANCE_RESERVATION_FAILED,
          payload: expect.objectContaining({
            payload: expect.objectContaining({
              reason: expect.stringContaining('Currency mismatch'),
            }),
          }),
        }),
        expect.any(Object)
      );
    });

    it('should reject with BalanceReservationFailed on missing balance projection and leave balance unchanged', async () => {
      const command = makeValidCommand();
      const payload = command.payload;

      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([]); // no projection

      const result = await balanceService.reserve(payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toContain('projection not found');
      expect(balanceRepository.reserveFunds).not.toHaveBeenCalled();

      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: BALANCE_RESERVATION_FAILED,
          payload: expect.objectContaining({
            payload: expect.objectContaining({
              reason: expect.stringContaining('projection not found'),
            }),
          }),
        }),
        expect.any(Object)
      );
    });

    it('should reject negative amounts in validation checks', async () => {
      const command = makeValidCommand();
      const payload = { ...command.payload, amount: -100 }; // invalid amount

      const result = await balanceService.reserve(payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toContain('Invalid balance reservation payload');
      expect(balanceRepository.reserveFunds).not.toHaveBeenCalled();
      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: BALANCE_RESERVATION_FAILED,
        }),
        expect.any(Object)
      );
    });

    it('should roll back the entire transaction if the outbox save fails', async () => {
      const command = makeValidCommand();
      const payload = command.payload;

      const mockBalance = new MerchantBalanceEntity(
        'balance_uuid',
        payload.merchantId,
        payload.currency,
        20000,
        0,
        new Date()
      );

      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([mockBalance]);
      jest.spyOn(balanceRepository, 'reserveFunds').mockResolvedValue(true);
      // Simulate outbox repository crash
      jest.spyOn(outboxRepository, 'save').mockRejectedValue(new Error('Outbox DB down'));

      await expect(balanceService.reserve(payload, command)).rejects.toThrow('Outbox DB down');
    });

    it('should assert concurrent reservations cannot overspend available balance', async () => {
      const commandA = makeValidCommand();
      const commandB = { ...makeValidCommand(), eventId: 'cmd_balance_456' };
      const payload = commandA.payload;

      const mockBalance = new MerchantBalanceEntity(
        'balance_uuid',
        payload.merchantId,
        payload.currency,
        20000, // Available is 20000, but each wants 15000
        0,
        new Date()
      );

      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([mockBalance]);

      // Serialized call execution
      // First call succeeds
      jest.spyOn(balanceRepository, 'reserveFunds')
        .mockResolvedValueOnce(true) // A succeeds
        .mockResolvedValueOnce(false); // B fails concurrently due to gte: 15000 check failing

      const resA = await balanceService.reserve(payload, commandA);
      const resB = await balanceService.reserve(payload, commandB);

      expect(resA.success).toBe(true);
      expect(resB.success).toBe(false);
      expect(resB.reason).toBe('Insufficient available balance');
    });
  });

  describe('BalanceService Reversal & Idempotency logic', () => {
    it('should release reserved balance, create reversal audit record, and write BALANCE_REVERSED outbox event', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      const mockBalance = new MerchantBalanceEntity(
        'balance_uuid',
        payload.merchantId,
        payload.currency,
        10000, // available
        15000, // reserved
        new Date()
      );

      jest.spyOn(balanceRepository, 'findReversalByPaymentId').mockResolvedValue(null);
      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([mockBalance]);
      jest.spyOn(balanceRepository, 'createReversal').mockResolvedValue({
        id: 'rev_audit_id',
        paymentId: payload.paymentId,
        merchantId: payload.merchantId,
        currency: payload.currency,
        amount: payload.amount,
        commandId: command.eventId,
        reversedAt: new Date(),
      });
      jest.spyOn(balanceRepository, 'releaseFunds').mockResolvedValue(true);

      const result = await balanceService.reverse(payload, command);

      expect(result.success).toBe(true);
      expect(balanceRepository.findReversalByPaymentId).toHaveBeenCalledWith(payload.paymentId, expect.any(Object));
      expect(balanceRepository.createReversal).toHaveBeenCalledWith(
        expect.objectContaining({
          paymentId: payload.paymentId,
          amount: payload.amount,
        }),
        expect.any(Object)
      );
      expect(balanceRepository.releaseFunds).toHaveBeenCalledWith(
        payload.merchantId,
        payload.currency,
        payload.amount,
        expect.any(Object)
      );
      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: BALANCE_REVERSED,
          aggregateId: 'balance_uuid',
        }),
        expect.any(Object)
      );
    });

    it('should skip idempotently (return success: true) if a reversal record already exists', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      const existingReversal = {
        id: 'rev_audit_id',
        paymentId: payload.paymentId,
        merchantId: payload.merchantId,
        currency: payload.currency,
        amount: payload.amount,
        commandId: 'prev_cmd_id',
        reversedAt: new Date(),
      };

      jest.spyOn(balanceRepository, 'findReversalByPaymentId').mockResolvedValue(existingReversal);

      const result = await balanceService.reverse(payload, command);

      expect(result.success).toBe(true);
      expect(balanceRepository.findByMerchantId).not.toHaveBeenCalled();
      expect(balanceRepository.releaseFunds).not.toHaveBeenCalled();
      expect(outboxRepository.save).not.toHaveBeenCalled();
    });

    it('should handle concurrent insert race via P2002 and return success: true', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      const mockBalance = new MerchantBalanceEntity(
        'balance_uuid',
        payload.merchantId,
        payload.currency,
        10000,
        15000,
        new Date()
      );

      jest.spyOn(balanceRepository, 'findReversalByPaymentId').mockResolvedValue(null);
      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([mockBalance]);

      const uniqueError: any = new Error('Unique constraint failed on paymentId');
      uniqueError.code = 'P2002';
      jest.spyOn(balanceRepository, 'createReversal').mockRejectedValue(uniqueError);

      const result = await balanceService.reverse(payload, command);

      expect(result.success).toBe(true); // race recovery returns success: true
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Concurrent balance reversal race'),
        expect.any(Object)
      );
    });

    it('should return success: false if merchant balance projection or currency mismatches', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      // No projections
      jest.spyOn(balanceRepository, 'findReversalByPaymentId').mockResolvedValue(null);
      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([]);

      const result = await balanceService.reverse(payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toContain('Merchant balance not found');
      expect(balanceRepository.createReversal).not.toHaveBeenCalled();
    });

    it('should return success: false if releaseFunds fails (insufficient reserved balance)', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      const mockBalance = new MerchantBalanceEntity(
        'balance_uuid',
        payload.merchantId,
        payload.currency,
        10000,
        5000, // reserved is 5000, but payload demands 15000
        new Date()
      );

      jest.spyOn(balanceRepository, 'findReversalByPaymentId').mockResolvedValue(null);
      jest.spyOn(balanceRepository, 'findByMerchantId').mockResolvedValue([mockBalance]);
      jest.spyOn(balanceRepository, 'createReversal').mockResolvedValue({} as any);
      jest.spyOn(balanceRepository, 'releaseFunds').mockResolvedValue(false); // fails!

      const result = await balanceService.reverse(payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toContain('Insufficient reserved balance for reversal');
      expect(outboxRepository.save).not.toHaveBeenCalled();
    });

    it('should reject invalid payload parameters in validation check', async () => {
      const command = makeValidReverseCommand();
      const payload = { ...command.payload, amount: -100 }; // invalid

      const result = await balanceService.reverse(payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toContain('Invalid balance reversal payload');
    });

    it('should re-throw random database errors to trigger retries', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      jest.spyOn(balanceRepository, 'findReversalByPaymentId').mockRejectedValue(new Error('DB Timeout'));

      await expect(balanceService.reverse(payload, command)).rejects.toThrow('DB Timeout');
    });
  });

  describe('OutboxRelayWorker publication', () => {
    it('should poll pending outbox events, publish to Kafka, and mark them as published', async () => {
      const mockOutboxRecord = {
        id: 'outbox_row_1',
        aggregateId: 'agg_bal_123',
        aggregateType: 'MerchantBalance',
        eventType: BALANCE_RESERVED,
        payload: { eventId: 'evt_reserved_123', payload: {} } as any,
        status: 'PENDING' as any,
        requestId: 'req_123',
        correlationId: 'corr_123',
        causationId: 'cause_123',
        createdAt: new Date(),
        publishedAt: null,
        retryCount: 0,
        traceHeaders: {},
      };

      jest.spyOn(outboxRepository, 'findPending').mockResolvedValue([mockOutboxRecord] as any);

      await outboxRelayWorker.processBatch();

      expect(outboxRepository.findPending).toHaveBeenCalled();
      expect(mockProducer.publish).toHaveBeenCalledWith(
        'balance.events',
        'agg_bal_123',
        mockOutboxRecord.payload,
        expect.any(Object)
      );
      expect(outboxRepository.markPublished).toHaveBeenCalledWith('outbox_row_1');
    });
  });
});
