import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService } from '@surgepay/common';
import {
  BALANCE_RESERVATION_FAILED,
  BALANCE_REVERSED,
  type BalanceReservationFailedEvent,
  type BalanceReversedEvent,
  ELIGIBILITY_DENIED,
  type EligibilityDeniedEvent,
  LEDGER_REVERSED,
  type LedgerReversedEvent,
  REVERSE_BALANCE,
  REVERSE_LEDGER_ENTRY,
  SAGA_STEP_EXECUTION_FAILED,
  type SagaStepExecutionFailedEvent,
} from '@surgepay/events';

import {
  OrderValidationStatus,
  SagaStatus,
  SagaTransitionType,
} from '../../generated/client';
import { CommandDispatcher } from '../dispatchers/command.dispatcher';
import { SagaInstanceEntity } from '../entities/saga-instance.entity';
import { SagaRepository } from '../repositories/saga.repository';
import {
  COMPENSATION_STEP_LABELS,
  CompensationCoordinator,
} from './compensation.coordinator';

// ─── Helpers ────────────────────────────────────────────────────────────────

function makeSaga(
  overrides: Partial<{
    status: SagaStatus;
    failureReason: string | null;
  }> = {}
): SagaInstanceEntity {
  const correlationId = 'test-correlation-id';
  return new SagaInstanceEntity(
    correlationId,
    'payment-1',
    correlationId,
    overrides.status ?? SagaStatus.ELIGIBILITY_PENDING,
    OrderValidationStatus.CONFIRMED,
    'merchant-1',
    1000,
    'USD',
    1,
    new Date(),
    null,
    new Date(),
    new Date(),
    overrides.failureReason ?? 'test failure',
    new Date(),
    'risk-engine',
    new Date(),
    0,
    null,
    null,
    null,
    null
  );
}

function makeEligibilityDeniedEvent(sagaId: string): EligibilityDeniedEvent {
  return {
    eventId: 'evt-elig-denied',
    eventType: ELIGIBILITY_DENIED as typeof ELIGIBILITY_DENIED,
    correlationId: sagaId,
    causationId: 'prior-event',
    sagaId,
    timestamp: new Date().toISOString(),
    version: 1,
    payload: { paymentId: 'payment-1', merchantId: 'merchant-1', reason: 'denied', deniedAt: new Date().toISOString() },
  };
}

function makeBalanceReservationFailedEvent(sagaId: string): BalanceReservationFailedEvent {
  return {
    eventId: 'evt-bal-failed',
    eventType: BALANCE_RESERVATION_FAILED as typeof BALANCE_RESERVATION_FAILED,
    correlationId: sagaId,
    causationId: 'prior-event',
    sagaId,
    timestamp: new Date().toISOString(),
    version: 1,
    payload: { paymentId: 'payment-1', merchantId: 'merchant-1', amount: 1000, currency: 'USD', reason: 'insufficient', failedAt: new Date().toISOString() },
  };
}

function makeStepExecutionFailedEvent(sagaId: string): SagaStepExecutionFailedEvent {
  return {
    eventId: 'evt-step-failed',
    eventType: SAGA_STEP_EXECUTION_FAILED as typeof SAGA_STEP_EXECUTION_FAILED,
    correlationId: sagaId,
    causationId: 'retry-scheduler',
    sagaId,
    timestamp: new Date().toISOString(),
    version: 1,
    payload: { sagaId, originalEventId: 'orig-evt', originalTopic: 'balance.commands', attempts: 5, failureReason: 'exhausted', failedAt: new Date().toISOString() },
  };
}

function makeBalanceReversedEvent(sagaId: string): BalanceReversedEvent {
  return {
    eventId: 'evt-bal-reversed',
    eventType: BALANCE_REVERSED as typeof BALANCE_REVERSED,
    correlationId: sagaId,
    causationId: 'reverse-balance-cmd',
    sagaId,
    timestamp: new Date().toISOString(),
    version: 1,
    payload: { reversalId: 'rev-1', paymentId: 'payment-1', merchantId: 'merchant-1', amount: 1000, currency: 'USD', reversedAt: new Date().toISOString() },
  };
}

function makeLedgerReversedEvent(sagaId: string): LedgerReversedEvent {
  return {
    eventId: 'evt-ledger-reversed',
    eventType: LEDGER_REVERSED as typeof LEDGER_REVERSED,
    correlationId: sagaId,
    causationId: 'reverse-ledger-cmd',
    sagaId,
    timestamp: new Date().toISOString(),
    version: 1,
    payload: { reversalEntryId: 'entry-rev-1', originalEntryId: 'entry-orig-1', paymentId: 'payment-1', merchantId: 'merchant-1', amount: 1000, currency: 'USD', reversedAt: new Date().toISOString() },
  };
}

// ─── Test Suite ─────────────────────────────────────────────────────────────

describe('CompensationCoordinator', () => {
  let coordinator: CompensationCoordinator;
  let sagaRepository: jest.Mocked<SagaRepository>;
  let commandDispatcher: jest.Mocked<CommandDispatcher>;

  beforeEach(async () => {
    const mockSagaRepository: jest.Mocked<Partial<SagaRepository>> = {
      update: jest.fn(async (saga) => saga),
      hasCompensationStep: jest.fn().mockResolvedValue(false),
    };

    const mockCommandDispatcher: jest.Mocked<Partial<CommandDispatcher>> = {
      dispatch: jest.fn().mockResolvedValue([{ partition: 0, offset: '0' }]),
    };

    const mockLogger = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CompensationCoordinator,
        { provide: SagaRepository, useValue: mockSagaRepository },
        { provide: CommandDispatcher, useValue: mockCommandDispatcher },
        { provide: LoggerService, useValue: mockLogger },
      ],
    }).compile();

    coordinator = module.get<CompensationCoordinator>(CompensationCoordinator);
    sagaRepository = module.get(SagaRepository);
    commandDispatcher = module.get(CommandDispatcher);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  // ─── classifyScenario ───────────────────────────────────────────────────

  describe('classifyScenario', () => {
    it('classifies ELIGIBILITY_PENDING as SCENARIO_1', () => {
      const saga = makeSaga({ status: SagaStatus.ELIGIBILITY_PENDING });
      expect(coordinator.classifyScenario(saga)).toBe('SCENARIO_1');
    });

    it('classifies BALANCE_PENDING as SCENARIO_2', () => {
      const saga = makeSaga({ status: SagaStatus.BALANCE_PENDING });
      expect(coordinator.classifyScenario(saga)).toBe('SCENARIO_2');
    });

    it('classifies BALANCE_RESERVED as SCENARIO_3', () => {
      const saga = makeSaga({ status: SagaStatus.BALANCE_RESERVED });
      expect(coordinator.classifyScenario(saga)).toBe('SCENARIO_3');
    });

    it('classifies LEDGER_PENDING as NONE', () => {
      const saga = makeSaga({ status: SagaStatus.LEDGER_PENDING });
      expect(coordinator.classifyScenario(saga)).toBe('NONE');
    });

    it('classifies LEDGER_RECORDED as NONE', () => {
      const saga = makeSaga({ status: SagaStatus.LEDGER_RECORDED });
      expect(coordinator.classifyScenario(saga)).toBe('NONE');
    });

    it('classifies REVERSED as NONE', () => {
      const saga = makeSaga({ status: SagaStatus.REVERSED });
      expect(coordinator.classifyScenario(saga)).toBe('NONE');
    });
  });

  // ─── initiateCompensation ───────────────────────────────────────────────

  describe('initiateCompensation', () => {
    it('Scenario 1: dispatches ReverseLedgerEntry and persists LEDGER_REVERSAL_DISPATCHED', async () => {
      const saga = makeSaga({ status: SagaStatus.ELIGIBILITY_PENDING });
      const event = makeEligibilityDeniedEvent(saga.id);

      await coordinator.initiateCompensation(saga, event);

      // Checkpoint persisted
      expect(sagaRepository.update).toHaveBeenCalledWith(saga, [
        expect.objectContaining({
          transitionType: SagaTransitionType.COMPENSATION_STEP,
          toState: COMPENSATION_STEP_LABELS.LEDGER_REVERSAL_DISPATCHED,
        }),
      ]);

      // Command dispatched
      expect(commandDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_LEDGER_ENTRY })
      );

      // ReverseBalance must NOT be dispatched for Scenario 1
      expect(commandDispatcher.dispatch).not.toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_BALANCE })
      );
    });

    it('Scenario 2: dispatches ReverseLedgerEntry (via BalanceReservationFailed trigger)', async () => {
      const saga = makeSaga({ status: SagaStatus.BALANCE_PENDING });
      const event = makeBalanceReservationFailedEvent(saga.id);

      await coordinator.initiateCompensation(saga, event);

      expect(commandDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_LEDGER_ENTRY })
      );
      expect(commandDispatcher.dispatch).not.toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_BALANCE })
      );
    });

    it('Scenario 2: dispatches ReverseLedgerEntry (via SagaStepExecutionFailed at BALANCE_PENDING)', async () => {
      const saga = makeSaga({ status: SagaStatus.BALANCE_PENDING });
      const event = makeStepExecutionFailedEvent(saga.id);

      await coordinator.initiateCompensation(saga, event);

      expect(commandDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_LEDGER_ENTRY })
      );
      expect(commandDispatcher.dispatch).not.toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_BALANCE })
      );
    });

    it('Scenario 3: dispatches ReverseBalance (not ReverseLedgerEntry) immediately', async () => {
      const saga = makeSaga({ status: SagaStatus.BALANCE_RESERVED });
      const event = makeStepExecutionFailedEvent(saga.id);

      await coordinator.initiateCompensation(saga, event);

      // Only ReverseBalance dispatched
      expect(commandDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_BALANCE })
      );

      // ReverseLedgerEntry must NOT be dispatched at initiation for Scenario 3
      expect(commandDispatcher.dispatch).not.toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_LEDGER_ENTRY })
      );

      // BALANCE_REVERSAL_DISPATCHED checkpoint must be persisted
      expect(sagaRepository.update).toHaveBeenCalledWith(saga, [
        expect.objectContaining({
          transitionType: SagaTransitionType.COMPENSATION_STEP,
          toState: COMPENSATION_STEP_LABELS.BALANCE_REVERSAL_DISPATCHED,
        }),
      ]);
    });

    it('NONE: no commands dispatched and no checkpoint persisted', async () => {
      const saga = makeSaga({ status: SagaStatus.LEDGER_PENDING });
      const event = makeStepExecutionFailedEvent(saga.id);

      await coordinator.initiateCompensation(saga, event);

      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
      expect(sagaRepository.update).not.toHaveBeenCalled();
    });
  });

  // ─── handleBalanceReversedAck ────────────────────────────────────────────

  describe('handleBalanceReversedAck', () => {
    it('Scenario 3: persists BALANCE_REVERSAL_ACKNOWLEDGED, dispatches ReverseLedgerEntry, persists LEDGER_REVERSAL_DISPATCHED', async () => {
      const saga = makeSaga({ status: SagaStatus.BALANCE_RESERVED });
      const event = makeBalanceReversedEvent(saga.id);

      // Simulate that BALANCE_REVERSAL_DISPATCHED checkpoint exists
      (sagaRepository.hasCompensationStep as jest.Mock).mockResolvedValue(true);

      await coordinator.handleBalanceReversedAck(saga, event);

      // BALANCE_REVERSAL_ACKNOWLEDGED checkpoint persisted first
      expect(sagaRepository.update).toHaveBeenCalledWith(
        saga,
        [expect.objectContaining({
          transitionType: SagaTransitionType.COMPENSATION_STEP,
          toState: COMPENSATION_STEP_LABELS.BALANCE_REVERSAL_ACKNOWLEDGED,
        })]
      );

      // ReverseLedgerEntry dispatched
      expect(commandDispatcher.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_LEDGER_ENTRY })
      );

      // ReverseBalance must NOT be dispatched again
      expect(commandDispatcher.dispatch).not.toHaveBeenCalledWith(
        expect.objectContaining({ eventType: REVERSE_BALANCE })
      );
    });

    it('skips idempotently if saga is already REVERSED', async () => {
      const saga = makeSaga({ status: SagaStatus.REVERSED });
      const event = makeBalanceReversedEvent(saga.id);

      await coordinator.handleBalanceReversedAck(saga, event);

      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
      expect(sagaRepository.update).not.toHaveBeenCalled();
    });

    it('skips idempotently if saga is already CLOSED', async () => {
      const saga = makeSaga({ status: SagaStatus.CLOSED });
      const event = makeBalanceReversedEvent(saga.id);

      await coordinator.handleBalanceReversedAck(saga, event);

      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
    });

    it('skips with warning if no BALANCE_REVERSAL_DISPATCHED checkpoint found', async () => {
      const saga = makeSaga({ status: SagaStatus.BALANCE_RESERVED });
      const event = makeBalanceReversedEvent(saga.id);

      // Checkpoint does not exist
      (sagaRepository.hasCompensationStep as jest.Mock).mockResolvedValue(false);

      await coordinator.handleBalanceReversedAck(saga, event);

      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
    });

    it('reverse-order invariant: ReverseLedgerEntry ONLY dispatched from handleBalanceReversedAck, not from initiateCompensation', async () => {
      const saga = makeSaga({ status: SagaStatus.BALANCE_RESERVED });
      const failEvent = makeStepExecutionFailedEvent(saga.id);

      // Phase 1: initiation — only ReverseBalance
      await coordinator.initiateCompensation(saga, failEvent);
      const dispatchCallsAfterInitiation = (commandDispatcher.dispatch as jest.Mock).mock.calls.map(
        (c) => c[0].eventType
      );
      expect(dispatchCallsAfterInitiation).not.toContain(REVERSE_LEDGER_ENTRY);
      expect(dispatchCallsAfterInitiation).toContain(REVERSE_BALANCE);

      // Phase 2: ack — only ReverseLedgerEntry
      (sagaRepository.hasCompensationStep as jest.Mock).mockResolvedValue(true);
      jest.clearAllMocks();

      const ackEvent = makeBalanceReversedEvent(saga.id);
      await coordinator.handleBalanceReversedAck(saga, ackEvent);
      const dispatchCallsAfterAck = (commandDispatcher.dispatch as jest.Mock).mock.calls.map(
        (c) => c[0].eventType
      );
      expect(dispatchCallsAfterAck).toContain(REVERSE_LEDGER_ENTRY);
      expect(dispatchCallsAfterAck).not.toContain(REVERSE_BALANCE);
    });
  });

  // ─── handleLedgerReversedAck ─────────────────────────────────────────────

  describe('handleLedgerReversedAck', () => {
    it('Scenario 1/2/3: transitions saga REVERSED then CLOSED and persists both', async () => {
      const saga = makeSaga({ status: SagaStatus.ELIGIBILITY_PENDING });
      const event = makeLedgerReversedEvent(saga.id);

      await coordinator.handleLedgerReversedAck(saga, event);

      expect(sagaRepository.update).toHaveBeenCalledWith(
        saga,
        expect.arrayContaining([
          expect.objectContaining({ transitionType: SagaTransitionType.SAGA_STATUS, toState: SagaStatus.REVERSED }),
          expect.objectContaining({ transitionType: SagaTransitionType.SAGA_STATUS, toState: SagaStatus.CLOSED }),
        ])
      );
      expect(saga.status).toBe(SagaStatus.CLOSED);
    });

    it('skips idempotently if saga is already REVERSED', async () => {
      const saga = makeSaga({ status: SagaStatus.REVERSED });
      const event = makeLedgerReversedEvent(saga.id);

      await coordinator.handleLedgerReversedAck(saga, event);

      expect(sagaRepository.update).not.toHaveBeenCalled();
    });

    it('skips idempotently if saga is already CLOSED', async () => {
      const saga = makeSaga({ status: SagaStatus.CLOSED });
      const event = makeLedgerReversedEvent(saga.id);

      await coordinator.handleLedgerReversedAck(saga, event);

      expect(sagaRepository.update).not.toHaveBeenCalled();
    });

    it('notification boundary: saga at NOTIFIED does not dispatch financial commands', async () => {
      const saga = makeSaga({ status: SagaStatus.NOTIFIED });
      const failEvent = makeStepExecutionFailedEvent(saga.id);

      // classifyScenario(NOTIFIED) → NONE → no commands dispatched
      await coordinator.initiateCompensation(saga, failEvent);

      expect(commandDispatcher.dispatch).not.toHaveBeenCalled();
    });

    it('command envelope carries correct correlationId and sagaId', async () => {
      const saga = makeSaga({ status: SagaStatus.ELIGIBILITY_PENDING });
      const event = makeEligibilityDeniedEvent(saga.id);

      await coordinator.initiateCompensation(saga, event);

      const dispatched = (commandDispatcher.dispatch as jest.Mock).mock.calls[0][0];
      expect(dispatched.correlationId).toBe(saga.correlationId);
      expect(dispatched.sagaId).toBe(saga.id);
      expect(dispatched.causationId).toBe(event.eventId);
    });
  });
});
