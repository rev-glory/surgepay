import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService } from '@surgepay/common';
import {
  BALANCE_RESERVATION_FAILED,
  BALANCE_RESERVED,
  type BalanceReservationFailedEvent,
  type BalanceReservedEvent,
  CHECK_PAYOUT_ELIGIBILITY,
  ELIGIBILITY_APPROVED,
  ELIGIBILITY_DENIED,
  type EligibilityApprovedEvent,
  type EligibilityDeniedEvent,
  LEDGER_ENTRY_RECORDED,
  LEDGER_RECORDING_FAILED,
  type LedgerEntryRecordedEvent,
  type LedgerRecordingFailedEvent,
  PAYMENT_COMPLETED,
  type PaymentCompletedEvent,
  RESERVE_BALANCE,
} from '@surgepay/events';

import { OrderValidationStatus, SagaStatus } from '../generated/client';
import { CommandDispatcher } from './dispatchers/command.dispatcher';
import { SagaInstanceEntity } from './entities/saga-instance.entity';
import { SagaRepository } from './repositories/saga.repository';
import { SagaService } from './saga.service';

describe('SagaService', () => {
  let service: SagaService;
  let loggerMock: jest.Mocked<Partial<LoggerService>>;
  let sagaRepositoryMock: {
    findByPaymentId: jest.Mock;
    findById: jest.Mock;
    create: jest.Mock;
    update: jest.Mock;
  };
  let commandDispatcherMock: {
    dispatch: jest.Mock;
  };

  beforeEach(async () => {
    loggerMock = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };
    sagaRepositoryMock = {
      findByPaymentId: jest.fn(),
      findById: jest.fn(),
      create: jest.fn(),
      update: jest.fn(),
    };
    commandDispatcherMock = {
      dispatch: jest.fn().mockResolvedValue([]),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SagaService,
        {
          provide: LoggerService,
          useValue: loggerMock,
        },
        {
          provide: SagaRepository,
          useValue: sagaRepositoryMock,
        },
        {
          provide: CommandDispatcher,
          useValue: commandDispatcherMock,
        },
      ],
    }).compile();

    service = module.get<SagaService>(SagaService);
  });

  const event: PaymentCompletedEvent = {
    eventId: 'evt_12345',
    eventType: PAYMENT_COMPLETED,
    correlationId: 'corr_54321',
    causationId: 'cause_99999',
    sagaId: 'corr_54321',
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

  it('should initialize and set correct logger context', () => {
    expect(loggerMock.setContext).toHaveBeenCalledWith('SagaService');
  });

  it('should create, persist, and dispatch commands when processPaymentCompleted is called', async () => {
    sagaRepositoryMock.findByPaymentId.mockResolvedValue(null);
    sagaRepositoryMock.create.mockResolvedValue(null as unknown as SagaInstanceEntity);

    await service.processPaymentCompleted(event);

    expect(sagaRepositoryMock.findByPaymentId).toHaveBeenCalledWith('pay_abc');
    expect(sagaRepositoryMock.create).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'corr_54321',
        paymentId: 'pay_abc',
        correlationId: 'corr_54321',
        status: SagaStatus.LEDGER_PENDING,
        merchantId: 'merch_xyz',
        amount: 5000,
        currency: 'USD',
      }),
      expect.any(Array)
    );
    expect(commandDispatcherMock.dispatch).toHaveBeenCalledTimes(2);
  });

  it('should skip creating a SagaInstance if one already exists for the payment', async () => {
    const existingSagaMock = {
      id: 'corr_54321',
      paymentId: 'pay_abc',
      correlationId: 'corr_54321',
    } as unknown as SagaInstanceEntity;
    sagaRepositoryMock.findByPaymentId.mockResolvedValue(existingSagaMock);

    await service.processPaymentCompleted(event);

    expect(sagaRepositoryMock.findByPaymentId).toHaveBeenCalledWith('pay_abc');
    expect(sagaRepositoryMock.create).not.toHaveBeenCalled();
    expect(commandDispatcherMock.dispatch).not.toHaveBeenCalled();
  });

  // --- LEDGER EVENTS TESTS ---

  describe('processLedgerEntryRecorded', () => {
    it('should transition status to ELIGIBILITY_PENDING and dispatch CheckPayoutEligibility command', async () => {
      const saga = SagaInstanceEntity.create({
        paymentId: 'pay_abc',
        correlationId: 'corr_54321',
        merchantId: 'merch_xyz',
        amount: 5000,
        currency: 'USD',
      });
      // Confirm the order first to satisfy invariant
      saga.confirmOrder();

      sagaRepositoryMock.findById.mockResolvedValue(saga);

      const ledgerEvent: LedgerEntryRecordedEvent = {
        eventId: 'evt_ledger_1',
        eventType: LEDGER_ENTRY_RECORDED,
        correlationId: 'corr_54321',
        causationId: 'cause_ledger_1',
        sagaId: 'corr_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          entryId: 'entry_1',
          paymentId: 'pay_abc',
          merchantId: 'merch_xyz',
          amount: 5000,
          currency: 'USD',
          recordedAt: new Date().toISOString(),
        },
      };

      await service.processLedgerEntryRecorded(ledgerEvent);

      expect(saga.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
      expect(sagaRepositoryMock.update).toHaveBeenCalledWith(saga, expect.any(Array));
      expect(commandDispatcherMock.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: CHECK_PAYOUT_ELIGIBILITY,
          payload: {
            paymentId: 'pay_abc',
            merchantId: 'merch_xyz',
            amount: 5000,
            currency: 'USD',
          },
        })
      );
    });

    it('should throw Error if order validation status is still PENDING', async () => {
      const saga = SagaInstanceEntity.create({
        paymentId: 'pay_abc',
        correlationId: 'corr_54321',
        merchantId: 'merch_xyz',
        amount: 5000,
        currency: 'USD',
      });
      sagaRepositoryMock.findById.mockResolvedValue(saga);

      const ledgerEvent: LedgerEntryRecordedEvent = {
        eventId: 'evt_ledger_1',
        eventType: LEDGER_ENTRY_RECORDED,
        correlationId: 'corr_54321',
        causationId: 'cause_ledger_1',
        sagaId: 'corr_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          entryId: 'entry_1',
          paymentId: 'pay_abc',
          merchantId: 'merch_xyz',
          amount: 5000,
          currency: 'USD',
          recordedAt: new Date().toISOString(),
        },
      };

      await expect(service.processLedgerEntryRecorded(ledgerEvent)).rejects.toThrow(
        /Cannot transition to LEDGER_RECORDED because order validation is PENDING/
      );
      expect(commandDispatcherMock.dispatch).not.toHaveBeenCalled();
    });

    it('should ignore duplicate event if state is already at or beyond LEDGER_RECORDED', async () => {
      const saga = new SagaInstanceEntity(
        'corr_54321',
        'pay_abc',
        'corr_54321',
        SagaStatus.ELIGIBILITY_PENDING,
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
      sagaRepositoryMock.findById.mockResolvedValue(saga);

      const ledgerEvent: LedgerEntryRecordedEvent = {
        eventId: 'evt_ledger_1',
        eventType: LEDGER_ENTRY_RECORDED,
        correlationId: 'corr_54321',
        causationId: 'cause_ledger_1',
        sagaId: 'corr_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          entryId: 'entry_1',
          paymentId: 'pay_abc',
          merchantId: 'merch_xyz',
          amount: 5000,
          currency: 'USD',
          recordedAt: new Date().toISOString(),
        },
      };

      await service.processLedgerEntryRecorded(ledgerEvent);

      expect(sagaRepositoryMock.update).not.toHaveBeenCalled();
      expect(commandDispatcherMock.dispatch).not.toHaveBeenCalled();
    });
  });

  describe('processLedgerRecordingFailed', () => {
    it('should set failure metadata but remain at LEDGER_PENDING and NOT transition to CLOSED', async () => {
      const saga = SagaInstanceEntity.create({
        paymentId: 'pay_abc',
        correlationId: 'corr_54321',
        merchantId: 'merch_xyz',
        amount: 5000,
        currency: 'USD',
      });
      sagaRepositoryMock.findById.mockResolvedValue(saga);

      const failedEvent: LedgerRecordingFailedEvent = {
        eventId: 'evt_failed_1',
        eventType: LEDGER_RECORDING_FAILED,
        correlationId: 'corr_54321',
        causationId: 'cause_failed_1',
        sagaId: 'corr_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId: 'pay_abc',
          merchantId: 'merch_xyz',
          amount: 5000,
          currency: 'USD',
          reason: 'Ledger connection timed out',
          failedAt: new Date().toISOString(),
        },
      };

      await service.processLedgerRecordingFailed(failedEvent);

      expect(saga.status).toBe(SagaStatus.LEDGER_PENDING);
      expect(saga.failureReason).toBe('Ledger recording failed: Ledger connection timed out');
      expect(saga.originService).toBe('ledger-service');
      expect(saga.failedAt).toBeInstanceOf(Date);
      expect(sagaRepositoryMock.update).toHaveBeenCalledWith(saga);
    });
  });

  // --- RISK EVENTS TESTS ---

  describe('processEligibilityApproved', () => {
    it('should transition status to BALANCE_PENDING and dispatch ReserveBalance command', async () => {
      const saga = new SagaInstanceEntity(
        'corr_54321',
        'pay_abc',
        'corr_54321',
        SagaStatus.ELIGIBILITY_PENDING,
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
      sagaRepositoryMock.findById.mockResolvedValue(saga);

      const riskEvent: EligibilityApprovedEvent = {
        eventId: 'evt_risk_1',
        eventType: ELIGIBILITY_APPROVED,
        correlationId: 'corr_54321',
        causationId: 'cause_risk_1',
        sagaId: 'corr_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId: 'pay_abc',
          merchantId: 'merch_xyz',
          approvedAt: new Date().toISOString(),
        },
      };

      await service.processEligibilityApproved(riskEvent);

      expect(saga.status).toBe(SagaStatus.BALANCE_PENDING);
      expect(sagaRepositoryMock.update).toHaveBeenCalledWith(saga, expect.any(Array));
      expect(commandDispatcherMock.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: RESERVE_BALANCE,
          payload: {
            paymentId: 'pay_abc',
            merchantId: 'merch_xyz',
            amount: 5000,
            currency: 'USD',
          },
        })
      );
    });

    it('should skip duplicate if status is already at or beyond BALANCE_PENDING', async () => {
      const saga = new SagaInstanceEntity(
        'corr_54321',
        'pay_abc',
        'corr_54321',
        SagaStatus.BALANCE_PENDING,
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
      sagaRepositoryMock.findById.mockResolvedValue(saga);

      const riskEvent: EligibilityApprovedEvent = {
        eventId: 'evt_risk_1',
        eventType: ELIGIBILITY_APPROVED,
        correlationId: 'corr_54321',
        causationId: 'cause_risk_1',
        sagaId: 'corr_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId: 'pay_abc',
          merchantId: 'merch_xyz',
          approvedAt: new Date().toISOString(),
        },
      };

      await service.processEligibilityApproved(riskEvent);

      expect(sagaRepositoryMock.update).not.toHaveBeenCalled();
      expect(commandDispatcherMock.dispatch).not.toHaveBeenCalled();
    });
  });

  describe('processEligibilityDenied', () => {
    it('should store failure details and block forward execution (Scenario 1 trigger)', async () => {
      const saga = new SagaInstanceEntity(
        'corr_54321',
        'pay_abc',
        'corr_54321',
        SagaStatus.ELIGIBILITY_PENDING,
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
      sagaRepositoryMock.findById.mockResolvedValue(saga);

      const riskEvent: EligibilityDeniedEvent = {
        eventId: 'evt_risk_2',
        eventType: ELIGIBILITY_DENIED,
        correlationId: 'corr_54321',
        causationId: 'cause_risk_2',
        sagaId: 'corr_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId: 'pay_abc',
          merchantId: 'merch_xyz',
          reason: 'High chargeback velocity',
          deniedAt: new Date().toISOString(),
        },
      };

      await service.processEligibilityDenied(riskEvent);

      expect(saga.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
      expect(saga.failureReason).toBe('Eligibility denied: High chargeback velocity');
      expect(saga.originService).toBe('risk-engine');
      expect(saga.failedAt).toBeInstanceOf(Date);
      expect(sagaRepositoryMock.update).toHaveBeenCalledWith(saga);

      // Invariant check: cannot proceed forward now
      expect(() => saga.transitionTo(SagaStatus.BALANCE_PENDING)).toThrow(
        /Cannot perform forward transition to BALANCE_PENDING when.*Saga has failed/
      );
    });
  });

  // --- BALANCE EVENTS TESTS ---

  describe('processBalanceReserved', () => {
    it('should transition status to BALANCE_RESERVED', async () => {
      const saga = new SagaInstanceEntity(
        'corr_54321',
        'pay_abc',
        'corr_54321',
        SagaStatus.BALANCE_PENDING,
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
      sagaRepositoryMock.findById.mockResolvedValue(saga);

      const balanceEvent: BalanceReservedEvent = {
        eventId: 'evt_bal_1',
        eventType: BALANCE_RESERVED,
        correlationId: 'corr_54321',
        causationId: 'cause_bal_1',
        sagaId: 'corr_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          reservationId: 'res_1',
          paymentId: 'pay_abc',
          merchantId: 'merch_xyz',
          amount: 5000,
          currency: 'USD',
          reservedAt: new Date().toISOString(),
        },
      };

      await service.processBalanceReserved(balanceEvent);

      expect(saga.status).toBe(SagaStatus.BALANCE_RESERVED);
      expect(sagaRepositoryMock.update).toHaveBeenCalledWith(saga, expect.any(Array));
    });
  });

  describe('processBalanceReservationFailed', () => {
    it('should store failure details and block forward execution (Scenario 2 trigger)', async () => {
      const saga = new SagaInstanceEntity(
        'corr_54321',
        'pay_abc',
        'corr_54321',
        SagaStatus.BALANCE_PENDING,
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
      sagaRepositoryMock.findById.mockResolvedValue(saga);

      const balanceEvent: BalanceReservationFailedEvent = {
        eventId: 'evt_bal_2',
        eventType: BALANCE_RESERVATION_FAILED,
        correlationId: 'corr_54321',
        causationId: 'cause_bal_2',
        sagaId: 'corr_54321',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId: 'pay_abc',
          merchantId: 'merch_xyz',
          amount: 5000,
          currency: 'USD',
          reason: 'Insufficient balance',
          failedAt: new Date().toISOString(),
        },
      };

      await service.processBalanceReservationFailed(balanceEvent);

      expect(saga.status).toBe(SagaStatus.BALANCE_PENDING);
      expect(saga.failureReason).toBe('Balance reservation failed: Insufficient balance');
      expect(saga.originService).toBe('balance-service');
      expect(saga.failedAt).toBeInstanceOf(Date);
      expect(sagaRepositoryMock.update).toHaveBeenCalledWith(saga);

      // Invariant check: cannot proceed forward now
      expect(() => saga.transitionTo(SagaStatus.BALANCE_RESERVED)).toThrow(
        /Cannot perform forward transition to BALANCE_RESERVED when.*Saga has failed/
      );
    });
  });
});
