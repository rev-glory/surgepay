import { Test, type TestingModule } from '@nestjs/testing';
import { randomUUID } from 'crypto';
import { LoggerService } from '@surgepay/common';
import {
  CHECK_PAYOUT_ELIGIBILITY,
  RECORD_LEDGER_ENTRY,
  RESERVE_BALANCE,
  REVERSE_BALANCE,
  REVERSE_LEDGER_ENTRY,
} from '@surgepay/events';

import { OrderValidationStatus, SagaStatus, SagaTransitionType } from '../../generated/client';
import { CompensationCoordinator } from '../compensation/compensation.coordinator';
import { CommandDispatcher } from '../dispatchers/command.dispatcher';
import { SagaInstanceEntity } from '../entities/saga-instance.entity';
import { SagaRepository } from '../repositories/saga.repository';
import { SagaRecoveryService } from './saga-recovery.service';

describe('SagaRecoveryService', () => {
  let recoveryService: SagaRecoveryService;
  let sagaRepository: jest.Mocked<SagaRepository>;
  let commandDispatcher: jest.Mocked<CommandDispatcher>;
  let compensationCoordinator: jest.Mocked<CompensationCoordinator>;

  const mockLogger = {
    setContext: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  };

  function createSagaEntity(overrides: Partial<SagaInstanceEntity> = {}): SagaInstanceEntity {
    const id = overrides.id || randomUUID();
    const paymentId = overrides.paymentId || randomUUID();
    const correlationId = id;
    return new SagaInstanceEntity(
      id,
      paymentId,
      correlationId,
      overrides.status ?? SagaStatus.LEDGER_PENDING,
      overrides.orderValidationStatus ?? OrderValidationStatus.PENDING,
      overrides.merchantId ?? 'test-merchant',
      overrides.amount ?? 100,
      overrides.currency ?? 'USD',
      overrides.version ?? 0,
      overrides.startedAt ?? new Date(),
      overrides.completedAt ?? null,
      overrides.createdAt ?? new Date(),
      overrides.updatedAt ?? new Date(),
      overrides.failureReason ?? null,
      overrides.failedAt ?? null,
      overrides.originService ?? null,
      overrides.stateUpdatedAt ?? new Date(),
      overrides.retryCount ?? 0,
      overrides.lastRetryAt ?? null,
      overrides.nextRetryAt ?? null,
      overrides.currentCommandId ?? null,
      overrides.retryHandoffAt ?? null,
      overrides.recoveredAt ?? null,
      overrides.recoveryCount ?? 0,
      overrides.recoveryReason ?? null
    );
  }

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SagaRecoveryService,
        {
          provide: SagaRepository,
          useValue: {
            findRecoverableSagas: jest.fn(),
            findCompensationStep: jest.fn(),
            update: jest.fn(),
          },
        },
        {
          provide: CommandDispatcher,
          useValue: {
            dispatch: jest.fn(),
          },
        },
        {
          provide: CompensationCoordinator,
          useValue: {
            initiateCompensation: jest.fn(),
          },
        },
        {
          provide: LoggerService,
          useValue: mockLogger,
        },
      ],
    }).compile();

    recoveryService = module.get<SagaRecoveryService>(SagaRecoveryService);
    sagaRepository = module.get(SagaRepository);
    commandDispatcher = module.get(CommandDispatcher);
    compensationCoordinator = module.get(CompensationCoordinator);

    jest.clearAllMocks();
  });

  describe('recoverIncompleteSagas', () => {
    it('should ignore closed/terminal sagas (they should not be returned by findRecoverableSagas)', async () => {
      sagaRepository.findRecoverableSagas.mockResolvedValue([]);

      await recoveryService.recoverIncompleteSagas();

      expect(sagaRepository.findRecoverableSagas).toHaveBeenCalled();
      expect(sagaRepository.update).not.toHaveBeenCalled();
      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
    });

    it('should skip sagas with active Retry Scheduler custody (nextRetryAt is not null)', async () => {
      const saga = createSagaEntity({
        nextRetryAt: new Date(),
      });
      sagaRepository.findRecoverableSagas.mockResolvedValue([saga]);

      await recoveryService.recoverIncompleteSagas();

      expect(sagaRepository.update).not.toHaveBeenCalled();
      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
    });

    it('should skip sagas with active Retry Scheduler custody (retryHandoffAt is not null)', async () => {
      const saga = createSagaEntity({
        retryHandoffAt: new Date(),
      });
      sagaRepository.findRecoverableSagas.mockResolvedValue([saga]);

      await recoveryService.recoverIncompleteSagas();

      expect(sagaRepository.update).not.toHaveBeenCalled();
      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
    });

    it('should resume LEDGER_RECORDED by transitioning to ELIGIBILITY_PENDING and dispatching CheckPayoutEligibility', async () => {
      const saga = createSagaEntity({
        status: SagaStatus.LEDGER_RECORDED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
      });
      sagaRepository.findRecoverableSagas.mockResolvedValue([saga]);
      sagaRepository.update.mockResolvedValue(saga);

      await recoveryService.recoverIncompleteSagas();

      expect(saga.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
      expect(saga.recoveryCount).toBe(1);
      expect(saga.recoveryReason).toContain('Advanced to ELIGIBILITY_PENDING');
      expect(sagaRepository.update).toHaveBeenCalledWith(saga, [
        {
          transitionType: SagaTransitionType.SAGA_STATUS,
          fromState: SagaStatus.LEDGER_RECORDED,
          toState: SagaStatus.ELIGIBILITY_PENDING,
          eventId: saga.currentCommandId!,
          causationId: saga.correlationId,
          eventType: CHECK_PAYOUT_ELIGIBILITY,
        },
      ]);
      expect(commandDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          eventId: saga.currentCommandId!,
          eventType: CHECK_PAYOUT_ELIGIBILITY,
          sagaId: saga.id,
          payload: {
            paymentId: saga.paymentId,
            merchantId: saga.merchantId,
            amount: saga.amount,
            currency: saga.currency,
          },
        })
      );
    });

    it('should skip BALANCE_RESERVED sagas (Day 5 forward flow boundary)', async () => {
      const saga = createSagaEntity({
        status: SagaStatus.BALANCE_RESERVED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
      });
      sagaRepository.findRecoverableSagas.mockResolvedValue([saga]);

      await recoveryService.recoverIncompleteSagas();

      expect(sagaRepository.update).not.toHaveBeenCalled();
      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
    });

    it('should transition REVERSED compensating sagas to CLOSED', async () => {
      const saga = createSagaEntity({
        status: SagaStatus.REVERSED,
        failureReason: 'Fraud scoring high risk',
      });
      sagaRepository.findRecoverableSagas.mockResolvedValue([saga]);
      sagaRepository.update.mockResolvedValue(saga);

      await recoveryService.recoverIncompleteSagas();

      expect(saga.status).toBe(SagaStatus.CLOSED);
      expect(saga.recoveryCount).toBe(1);
      expect(saga.recoveryReason).toContain('Completed compensation flow closed');
      expect(sagaRepository.update).toHaveBeenCalledWith(saga);
      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
    });

    it('should redispatch ReverseBalance if BALANCE_REVERSAL_DISPATCHED checkpoint exists but not BALANCE_REVERSAL_ACKNOWLEDGED', async () => {
      const saga = createSagaEntity({
        status: SagaStatus.BALANCE_PENDING,
        failureReason: 'Fraud scoring high risk',
      });
      const checkpointCmdId = 'balance-rev-cmd-id';
      sagaRepository.findRecoverableSagas.mockResolvedValue([saga]);
      sagaRepository.findCompensationStep.mockImplementation(async (sagaId, step) => {
        if (step === 'BALANCE_REVERSAL_DISPATCHED') {
          return { eventId: checkpointCmdId } as any;
        }
        return null;
      });

      await recoveryService.recoverIncompleteSagas();

      expect(saga.recoveryCount).toBe(1);
      expect(saga.recoveryReason).toContain('Resumed and redispatched ReverseBalance');
      expect(sagaRepository.update).toHaveBeenCalledWith(saga);
      expect(commandDispatcher.dispatch).toHaveBeenCalledWith({
        eventId: checkpointCmdId,
        eventType: REVERSE_BALANCE,
        correlationId: saga.correlationId,
        causationId: checkpointCmdId,
        sagaId: saga.id,
        timestamp: expect.any(String),
        version: 1,
        payload: {
          paymentId: saga.paymentId,
          merchantId: saga.merchantId,
          amount: saga.amount,
          currency: saga.currency,
          reason: saga.failureReason,
        },
      });
    });

    it('should dispatch ReverseLedgerEntry if BALANCE_REVERSAL_ACKNOWLEDGED exists but not LEDGER_REVERSAL_DISPATCHED', async () => {
      const saga = createSagaEntity({
        status: SagaStatus.BALANCE_PENDING,
        failureReason: 'Fraud scoring high risk',
      });
      const ackEventId = 'balance-rev-ack-id';
      sagaRepository.findRecoverableSagas.mockResolvedValue([saga]);
      sagaRepository.findCompensationStep.mockImplementation(async (sagaId, step) => {
        if (step === 'BALANCE_REVERSAL_ACKNOWLEDGED') {
          return { eventId: ackEventId } as any;
        }
        return null;
      });

      await recoveryService.recoverIncompleteSagas();

      expect(saga.recoveryCount).toBe(1);
      expect(saga.recoveryReason).toContain('Resumed and dispatched ReverseLedgerEntry');
      expect(sagaRepository.update).toHaveBeenCalledWith(
        saga,
        expect.arrayContaining([
          expect.objectContaining({
            transitionType: SagaTransitionType.COMPENSATION_STEP,
            toState: 'LEDGER_REVERSAL_DISPATCHED',
            causationId: ackEventId,
            eventType: REVERSE_LEDGER_ENTRY,
          }),
        ])
      );
      expect(commandDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          eventType: REVERSE_LEDGER_ENTRY,
          causationId: ackEventId,
          sagaId: saga.id,
        })
      );
    });

    it('should redispatch ReverseLedgerEntry if LEDGER_REVERSAL_DISPATCHED exists but status is not REVERSED', async () => {
      const saga = createSagaEntity({
        status: SagaStatus.BALANCE_PENDING,
        failureReason: 'Fraud scoring high risk',
      });
      const ledgerRevCmdId = 'ledger-rev-cmd-id';
      sagaRepository.findRecoverableSagas.mockResolvedValue([saga]);
      sagaRepository.findCompensationStep.mockImplementation(async (sagaId, step) => {
        if (step === 'LEDGER_REVERSAL_DISPATCHED') {
          return { eventId: ledgerRevCmdId } as any;
        }
        return null;
      });

      await recoveryService.recoverIncompleteSagas();

      expect(saga.recoveryCount).toBe(1);
      expect(saga.recoveryReason).toContain('Resumed and redispatched ReverseLedgerEntry');
      expect(sagaRepository.update).toHaveBeenCalledWith(saga);
      expect(commandDispatcher.dispatch).toHaveBeenCalledWith({
        eventId: ledgerRevCmdId,
        eventType: REVERSE_LEDGER_ENTRY,
        correlationId: saga.correlationId,
        causationId: ledgerRevCmdId,
        sagaId: saga.id,
        timestamp: expect.any(String),
        version: 1,
        payload: {
          paymentId: saga.paymentId,
          merchantId: saga.merchantId,
          amount: saga.amount,
          currency: saga.currency,
          reason: saga.failureReason,
        },
      });
    });

    it('should isolate failures (an error in saga A does not block saga B)', async () => {
      const sagaA = createSagaEntity({
        id: 'saga-a',
        status: SagaStatus.LEDGER_RECORDED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
      });
      const sagaB = createSagaEntity({
        id: 'saga-b',
        status: SagaStatus.LEDGER_RECORDED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
      });
      sagaRepository.findRecoverableSagas.mockResolvedValue([sagaA, sagaB]);

      // Make update throw for saga A
      sagaRepository.update.mockRejectedValueOnce(new Error('DB connection failed for saga A'));
      sagaRepository.update.mockResolvedValue(sagaB);

      await recoveryService.recoverIncompleteSagas();

      // Saga A failed, but Saga B should have succeeded
      expect(sagaRepository.update).toHaveBeenCalledTimes(2);
      expect(sagaB.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
    });
  });
});
