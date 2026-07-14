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
  LEDGER_ENTRY_RECORDED,
  LEDGER_RECORDING_FAILED,
  LEDGER_REVERSED,
  RECORD_LEDGER_ENTRY,
  type RecordLedgerEntryCommand,
  REVERSE_LEDGER_ENTRY,
  type ReverseLedgerEntryCommand,
} from '@surgepay/events';

import { LedgerEntryEntity } from '../entities/ledger-entry.entity';
import { LedgerEntryType } from '../generated/client';
import { PrismaService } from '../prisma/prisma.service';
import { LedgerInboxRepository } from '../repositories/inbox.repository';
import { LedgerRepository } from '../repositories/ledger.repository';
import { OutboxRepository } from '../repositories/outbox.repository';
import { LedgerService } from '../services/ledger.service';
import { OutboxRelayWorker } from '../services/outbox-relay.worker';
import { LedgerCommandConsumer } from './ledger-command.consumer';

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

describe('Ledger Command Handling & Domain Logic Spec', () => {
  let consumer: LedgerCommandConsumer;
  let ledgerService: LedgerService;
  let ledgerRepository: LedgerRepository;
  let outboxRepository: OutboxRepository;
  let inboxRepository: LedgerInboxRepository;
  let prismaService: PrismaService;
  let outboxRelayWorker: OutboxRelayWorker;

  // Mocked dependencies
  const mockProducer = {
    publish: jest.fn().mockResolvedValue([{ topic: 'ledger.events', partition: 0, offset: '1' }]),
  };

  const mockLogger = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    setContext: jest.fn(),
  };

  const mockConfig = {
    kafka: {
      consumerGroupId: 'test-group',
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
        LedgerCommandConsumer,
        LedgerService,
        {
          provide: LedgerRepository,
          useValue: {
            create: jest.fn(),
            findBySourceCommandId: jest.fn(),
            findById: jest.fn(),
            findByPaymentId: jest.fn(),
            findOriginalByPaymentId: jest.fn(),
            findCompensationByOriginalEntryId: jest.fn(),
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
          provide: LedgerInboxRepository,
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

    consumer = module.get<LedgerCommandConsumer>(LedgerCommandConsumer);
    ledgerService = module.get<LedgerService>(LedgerService);
    ledgerRepository = module.get<LedgerRepository>(LedgerRepository);
    outboxRepository = module.get<OutboxRepository>(OutboxRepository);
    inboxRepository = module.get<LedgerInboxRepository>(LedgerInboxRepository);
    prismaService = module.get<PrismaService>(PrismaService);
    outboxRelayWorker = module.get<OutboxRelayWorker>(OutboxRelayWorker);
  });

  const makeValidCommand = (): RecordLedgerEntryCommand => ({
    eventId: 'cmd_123',
    eventType: RECORD_LEDGER_ENTRY,
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
      entryType: 'DEBIT',
      description: 'Test settlement',
    },
  });

  const makeValidReverseCommand = (): ReverseLedgerEntryCommand => ({
    eventId: 'cmd_rev_123',
    eventType: REVERSE_LEDGER_ENTRY,
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

  describe('LedgerCommandConsumer handleEvent', () => {
    it('should delegate valid RecordLedgerEntry command to LedgerService', async () => {
      const command = makeValidCommand();
      jest.spyOn(ledgerService, 'recordEntry').mockResolvedValue({
        success: true,
        entry: new LedgerEntryEntity(
          'entry_uuid',
          command.payload.paymentId,
          command.payload.merchantId,
          command.payload.amount,
          command.payload.currency,
          LedgerEntryType.DEBIT,
          command.payload.description,
          new Date(),
          command.eventId,
          command.correlationId,
          command.causationId,
          command.sagaId
        ),
      });

      // Invoke the protected handler directly
      await (consumer as any).handleEvent(command);

      expect(ledgerService.recordEntry).toHaveBeenCalledWith(command.payload, command);
      expect(mockLogger.info).toHaveBeenCalledWith(
        'RecordLedgerEntry processed successfully',
        expect.any(Object)
      );
    });

    it('should ignore unsupported event types cleanly', async () => {
      const command = { ...makeValidCommand(), eventType: 'UnsupportedType' };
      const recordEntrySpy = jest.spyOn(ledgerService, 'recordEntry');
      await (consumer as any).handleEvent(command);

      expect(recordEntrySpy).not.toHaveBeenCalled();
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
      jest.spyOn(ledgerService, 'recordEntry').mockResolvedValue({
        success: false,
        reason: 'Invalid merchant boundary check',
      });

      await (consumer as any).handleEvent(command);

      expect(ledgerService.recordEntry).toHaveBeenCalledWith(command.payload, command);
      expect(mockLogger.error).toHaveBeenCalledWith(
        'RecordLedgerEntry failed permanently',
        expect.any(Object)
      );
    });

    it('should delegate valid ReverseLedgerEntry command to LedgerService', async () => {
      const command = makeValidReverseCommand();
      jest.spyOn(ledgerService, 'reverseEntry').mockResolvedValue({
        success: true,
        reversalEntry: new LedgerEntryEntity(
          'reversal_entry_uuid',
          command.payload.paymentId,
          command.payload.merchantId,
          command.payload.amount,
          command.payload.currency,
          LedgerEntryType.CREDIT,
          'Reversal description',
          new Date(),
          command.eventId,
          command.correlationId,
          command.causationId,
          command.sagaId,
          'orig_entry_id'
        ),
      });

      await (consumer as any).handleEvent(command);

      expect(ledgerService.reverseEntry).toHaveBeenCalledWith(command.payload, command);
      expect(mockLogger.info).toHaveBeenCalledWith(
        'ReverseLedgerEntry processed successfully',
        expect.any(Object)
      );
    });

    it('should throw MalformedEventEnvelopeException if ReverseLedgerEntry payload lacks reason', async () => {
      const command = makeValidReverseCommand();
      command.payload.reason = undefined as any;

      await expect((consumer as any).handleEvent(command)).rejects.toThrow(
        MalformedEventEnvelopeException
      );
    });

    it('should finish cleanly and not bubble up exception on ReverseLedgerEntry permanent failure', async () => {
      const command = makeValidReverseCommand();
      jest.spyOn(ledgerService, 'reverseEntry').mockResolvedValue({
        success: false,
        reason: 'No original ledger entry found',
      });

      await (consumer as any).handleEvent(command);

      expect(ledgerService.reverseEntry).toHaveBeenCalledWith(command.payload, command);
      expect(mockLogger.error).toHaveBeenCalledWith(
        'ReverseLedgerEntry failed permanently',
        expect.any(Object)
      );
    });
  });

  describe('LedgerService recordEntry & Idempotency logic', () => {
    it('should perform validations, create a ledger entry, and save to outbox on success', async () => {
      const command = makeValidCommand();
      const payload = command.payload;

      const createdEntryEntity = new LedgerEntryEntity(
        'entry_id_999',
        payload.paymentId,
        payload.merchantId,
        payload.amount,
        payload.currency,
        LedgerEntryType.DEBIT,
        payload.description,
        new Date(),
        command.eventId,
        command.correlationId,
        command.causationId,
        command.sagaId
      );

      jest.spyOn(ledgerRepository, 'create').mockResolvedValue(createdEntryEntity);

      const result = await ledgerService.recordEntry(payload, command);

      expect(result.success).toBe(true);
      expect(result.entry?.id).toBe('entry_id_999');
      expect(ledgerRepository.create).toHaveBeenCalled();
      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: LEDGER_ENTRY_RECORDED,
          aggregateId: 'entry_id_999',
        }),
        expect.any(Object)
      );
    });

    it('should write LedgerRecordingFailed to outbox and return success: false on invalid amount payload', async () => {
      const command = makeValidCommand();
      const payload = { ...command.payload, amount: -100 }; // invalid

      const result = await ledgerService.recordEntry(payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toContain('Invalid ledger entry payload');
      expect(ledgerRepository.create).not.toHaveBeenCalled();
      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: LEDGER_RECORDING_FAILED,
        }),
        expect.any(Object)
      );
    });

    it('should return idempotent success when unique constraint P2002 collision matches existing entry', async () => {
      const command = makeValidCommand();
      const payload = command.payload;

      const existingEntry = new LedgerEntryEntity(
        'existing_entry_123',
        payload.paymentId,
        payload.merchantId,
        payload.amount,
        payload.currency,
        LedgerEntryType.DEBIT,
        payload.description,
        new Date(),
        command.eventId,
        command.correlationId,
        command.causationId,
        command.sagaId
      );

      // Simulate P2002 unique constraint error on ledgerRepository.create
      const uniqueError: any = new Error('Unique constraint failed');
      uniqueError.code = 'P2002';
      jest.spyOn(ledgerRepository, 'create').mockRejectedValue(uniqueError);
      jest.spyOn(ledgerRepository, 'findBySourceCommandId').mockResolvedValue(existingEntry);

      const result = await ledgerService.recordEntry(payload, command);

      expect(result.success).toBe(true);
      expect(result.entry?.id).toBe('existing_entry_123');
      expect(ledgerRepository.findBySourceCommandId).toHaveBeenCalledWith(command.eventId);
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Idempotency collision hit'),
        expect.any(Object)
      );
    });

    it('should write LedgerRecordingFailed and return failure when unique constraint P2002 collision has mismatching fields', async () => {
      const command = makeValidCommand();
      const payload = command.payload;

      const mismatchedEntry = new LedgerEntryEntity(
        'existing_entry_123',
        payload.paymentId,
        payload.merchantId,
        999999, // Mismatched amount!
        payload.currency,
        LedgerEntryType.DEBIT,
        payload.description,
        new Date(),
        command.eventId,
        command.correlationId,
        command.causationId,
        command.sagaId
      );

      const uniqueError: any = new Error('Unique constraint failed');
      uniqueError.code = 'P2002';
      jest.spyOn(ledgerRepository, 'create').mockRejectedValue(uniqueError);
      jest.spyOn(ledgerRepository, 'findBySourceCommandId').mockResolvedValue(mismatchedEntry);

      const result = await ledgerService.recordEntry(payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toContain('payload does not match existing entry');
      expect(mockLogger.error).toHaveBeenCalledWith(
        expect.stringContaining('collision hit but business/financial payload does not match'),
        expect.any(Object)
      );
      // Writes failure event to outbox
      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: LEDGER_RECORDING_FAILED,
        }),
        expect.any(Object)
      );
    });

    it('should re-throw non-P2002 db errors to trigger consumer retries', async () => {
      const command = makeValidCommand();
      const payload = command.payload;

      const randomDbError = new Error('Connection timeout');
      jest.spyOn(ledgerRepository, 'create').mockRejectedValue(randomDbError);

      await expect(ledgerService.recordEntry(payload, command)).rejects.toThrow(
        'Connection timeout'
      );
    });
  });

  describe('LedgerService reverseEntry & Idempotency logic', () => {
    it('should fail cleanly when original ledger entry is not found', async () => {
      const command = makeValidReverseCommand();
      jest.spyOn(ledgerRepository, 'findOriginalByPaymentId').mockResolvedValue(null);

      const result = await ledgerService.reverseEntry(command.payload, command);

      expect(result.success).toBe(false);
      expect(result.reason).toContain('No original ledger entry found');
      expect(ledgerRepository.create).not.toHaveBeenCalled();
      expect(outboxRepository.save).not.toHaveBeenCalled();
    });

    it('should create CREDIT entry with reversalOf referencing original entry and write LEDGER_REVERSED to outbox', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      const originalEntry = new LedgerEntryEntity(
        'orig_entry_id',
        payload.paymentId,
        payload.merchantId,
        payload.amount,
        payload.currency,
        LedgerEntryType.DEBIT,
        'original DEBIT',
        new Date(),
        'orig_cmd_123',
        command.correlationId,
        command.causationId,
        command.sagaId
      );

      const reversalEntry = new LedgerEntryEntity(
        'rev_entry_id',
        payload.paymentId,
        payload.merchantId,
        payload.amount,
        payload.currency,
        LedgerEntryType.CREDIT,
        'compensation CREDIT',
        new Date(),
        command.eventId,
        command.correlationId,
        command.causationId,
        command.sagaId,
        'orig_entry_id'
      );

      jest.spyOn(ledgerRepository, 'findOriginalByPaymentId').mockResolvedValue(originalEntry);
      jest.spyOn(ledgerRepository, 'findCompensationByOriginalEntryId').mockResolvedValue(null);
      jest.spyOn(ledgerRepository, 'create').mockResolvedValue(reversalEntry);

      const result = await ledgerService.reverseEntry(payload, command);

      expect(result.success).toBe(true);
      expect(result.reversalEntry?.reversalOf).toBe('orig_entry_id');
      expect(ledgerRepository.create).toHaveBeenCalledWith(
        expect.objectContaining({
          entryType: 'CREDIT',
          reversalOf: 'orig_entry_id',
        }),
        expect.any(Object)
      );
      expect(outboxRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: LEDGER_REVERSED,
          aggregateId: 'rev_entry_id',
        }),
        expect.any(Object)
      );
    });

    it('should return existing compensation entry (idempotency skip) if it already exists', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      const originalEntry = new LedgerEntryEntity(
        'orig_entry_id',
        payload.paymentId,
        payload.merchantId,
        payload.amount,
        payload.currency,
        LedgerEntryType.DEBIT,
        'original DEBIT',
        new Date(),
        'orig_cmd_123',
        command.correlationId,
        command.causationId,
        command.sagaId
      );

      const existingReversal = new LedgerEntryEntity(
        'existing_rev_entry_id',
        payload.paymentId,
        payload.merchantId,
        payload.amount,
        payload.currency,
        LedgerEntryType.CREDIT,
        'already compensated',
        new Date(),
        'other_cmd_eventId',
        command.correlationId,
        command.causationId,
        command.sagaId,
        'orig_entry_id'
      );

      jest.spyOn(ledgerRepository, 'findOriginalByPaymentId').mockResolvedValue(originalEntry);
      jest.spyOn(ledgerRepository, 'findCompensationByOriginalEntryId').mockResolvedValue(existingReversal);

      const result = await ledgerService.reverseEntry(payload, command);

      expect(result.success).toBe(true);
      expect(result.reversalEntry?.id).toBe('existing_rev_entry_id');
      expect(ledgerRepository.create).not.toHaveBeenCalled();
      expect(outboxRepository.save).not.toHaveBeenCalled();
    });

    it('should handle concurrent insert race via P2002 unique index violation and return existing compensation', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      const originalEntry = new LedgerEntryEntity(
        'orig_entry_id',
        payload.paymentId,
        payload.merchantId,
        payload.amount,
        payload.currency,
        LedgerEntryType.DEBIT,
        'original DEBIT',
        new Date(),
        'orig_cmd_123',
        command.correlationId,
        command.causationId,
        command.sagaId
      );

      const existingReversal = new LedgerEntryEntity(
        'existing_rev_entry_id',
        payload.paymentId,
        payload.merchantId,
        payload.amount,
        payload.currency,
        LedgerEntryType.CREDIT,
        'already compensated',
        new Date(),
        'other_cmd_eventId',
        command.correlationId,
        command.causationId,
        command.sagaId,
        'orig_entry_id'
      );

      jest.spyOn(ledgerRepository, 'findOriginalByPaymentId').mockResolvedValue(originalEntry);
      jest.spyOn(ledgerRepository, 'findCompensationByOriginalEntryId')
        .mockResolvedValueOnce(null) // first check sees nothing
        .mockResolvedValueOnce(existingReversal); // P2002 lookup finds it

      const uniqueError: any = new Error('Unique constraint failed on reversalOf');
      uniqueError.code = 'P2002';
      jest.spyOn(ledgerRepository, 'create').mockRejectedValue(uniqueError);

      const result = await ledgerService.reverseEntry(payload, command);

      expect(result.success).toBe(true);
      expect(result.reversalEntry?.id).toBe('existing_rev_entry_id');
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Concurrent ledger reversal race'),
        expect.any(Object)
      );
    });

    it('should re-throw non-P2002 db errors on reverseEntry', async () => {
      const command = makeValidReverseCommand();
      const payload = command.payload;

      const originalEntry = new LedgerEntryEntity(
        'orig_entry_id',
        payload.paymentId,
        payload.merchantId,
        payload.amount,
        payload.currency,
        LedgerEntryType.DEBIT,
        'original DEBIT',
        new Date(),
        'orig_cmd_123',
        command.correlationId,
        command.causationId,
        command.sagaId
      );

      jest.spyOn(ledgerRepository, 'findOriginalByPaymentId').mockResolvedValue(originalEntry);
      jest.spyOn(ledgerRepository, 'findCompensationByOriginalEntryId').mockResolvedValue(null);

      const randomDbError = new Error('Connection timeout');
      jest.spyOn(ledgerRepository, 'create').mockRejectedValue(randomDbError);

      await expect(ledgerService.reverseEntry(payload, command)).rejects.toThrow(
        'Connection timeout'
      );
    });
  });

  describe('OutboxRelayWorker publication', () => {
    it('should poll pending outbox events, publish to Kafka, and mark them as published', async () => {
      const mockOutboxRecord = {
        id: 'outbox_row_1',
        aggregateId: 'agg_pay_123',
        aggregateType: 'LedgerEntry',
        eventType: LEDGER_ENTRY_RECORDED,
        payload: { eventId: 'evt_recorded_123', payload: {} } as any,
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
        'ledger.events',
        'agg_pay_123',
        mockOutboxRecord.payload,
        expect.any(Object)
      );
      expect(outboxRepository.markPublished).toHaveBeenCalledWith('outbox_row_1');
    });

    it('should support duplicate-publication recovery test scenario', async () => {
      const mockOutboxRecord = {
        id: 'outbox_row_failed_mark',
        aggregateId: 'agg_pay_456',
        aggregateType: 'LedgerEntry',
        eventType: LEDGER_ENTRY_RECORDED,
        payload: { eventId: 'evt_recorded_456', payload: {} } as any,
        status: 'PENDING' as any,
        requestId: 'req_456',
        correlationId: 'corr_456',
        causationId: 'cause_456',
        createdAt: new Date(),
        publishedAt: null,
        retryCount: 0,
        traceHeaders: {},
      };

      jest.spyOn(outboxRepository, 'findPending').mockResolvedValue([mockOutboxRecord] as any);

      // Simulates successful Kafka publication, but then the DB markPublished fails/crashes
      mockProducer.publish.mockResolvedValueOnce([{ topic: 'ledger.events', partition: 0, offset: '1' }]);
      jest.spyOn(outboxRepository, 'markPublished').mockRejectedValueOnce(new Error('Prisma disconnect'));

      // Process batch throws/handles the error gracefully for this item, incrementing retry
      await outboxRelayWorker.processBatch();

      expect(mockProducer.publish).toHaveBeenCalled();
      expect(outboxRepository.incrementRetry).toHaveBeenCalledWith('outbox_row_failed_mark');

      // Next poll, the worker picks it up again and successfully publishes it again
      const mockOutboxRecordRetry = { ...mockOutboxRecord, retryCount: 1 };
      jest.spyOn(outboxRepository, 'findPending').mockResolvedValue([mockOutboxRecordRetry] as any);
      jest.spyOn(outboxRepository, 'markPublished').mockResolvedValue({} as any);

      await outboxRelayWorker.processBatch();

      expect(mockProducer.publish).toHaveBeenCalledTimes(2); // Published again
      expect(outboxRepository.markPublished).toHaveBeenCalledWith('outbox_row_failed_mark');
      // Assert that duplicate publication is acceptable in event-driven systems (downstream inbox deduplicates it)
      expect(mockLogger.info).toHaveBeenCalledWith(
        'Event published successfully to Kafka',
        expect.objectContaining({ outboxEventId: 'outbox_row_failed_mark' })
      );
    });
  });

  describe('LedgerRepository Append-Only Invariant', () => {
    it('should not expose any update, delete, or overwrite methods on the repository class', () => {
      const prototype = Object.getPrototypeOf(ledgerRepository);
      const methodNames = Object.getOwnPropertyNames(prototype);

      const hasUpdate = methodNames.some((m) => m.toLowerCase().includes('update'));
      const hasDelete = methodNames.some((m) => m.toLowerCase().includes('delete'));
      const hasRemove = methodNames.some((m) => m.toLowerCase().includes('remove'));
      const hasSave = methodNames.some((m) => m.toLowerCase() === 'save');

      expect(hasUpdate).toBe(false);
      expect(hasDelete).toBe(false);
      expect(hasRemove).toBe(false);
      expect(hasSave).toBe(false);
    });
  });
});
