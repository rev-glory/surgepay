import { randomUUID } from 'crypto';

import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import {
  type BalanceReservationFailedEvent,
  type BalanceReversedEvent,
  type EligibilityDeniedEvent,
  type LedgerReversedEvent,
  REVERSE_BALANCE,
  REVERSE_LEDGER_ENTRY,
  type SagaStepExecutionFailedEvent,
} from '@surgepay/events';

import { SagaStatus, SagaTransitionType } from '../../generated/client';
import { CommandDispatcher } from '../dispatchers/command.dispatcher';
import { SagaInstanceEntity } from '../entities/saga-instance.entity';
import { SagaRepository } from '../repositories/saga.repository';

/**
 * Compensation scenario classification — derived from saga.status at the time of failure.
 * This enum is internal to the compensation layer and is not persisted.
 *
 * SCENARIO_1: saga at ELIGIBILITY_PENDING — only LedgerEntryRecorded completed.
 *   → Dispatch ReverseLedgerEntry immediately.
 * SCENARIO_2: saga at BALANCE_PENDING — only LedgerEntryRecorded completed.
 *   → Dispatch ReverseLedgerEntry immediately.
 * SCENARIO_3: saga at BALANCE_RESERVED — both LedgerEntryRecorded and BalanceReserved completed.
 *   → Dispatch ReverseBalance first; dispatch ReverseLedgerEntry only after BalanceReversed ack.
 * NONE: saga at LEDGER_PENDING or any terminal state — nothing succeeded, nothing to compensate.
 */
export type CompensationScenario = 'SCENARIO_1' | 'SCENARIO_2' | 'SCENARIO_3' | 'NONE';

/** Labels used for COMPENSATION_STEP SagaTransition records. */
export const COMPENSATION_STEP_LABELS = {
  BALANCE_REVERSAL_DISPATCHED: 'BALANCE_REVERSAL_DISPATCHED',
  BALANCE_REVERSAL_ACKNOWLEDGED: 'BALANCE_REVERSAL_ACKNOWLEDGED',
  LEDGER_REVERSAL_DISPATCHED: 'LEDGER_REVERSAL_DISPATCHED',
} as const;

type TriggeringEvent =
  | EligibilityDeniedEvent
  | BalanceReservationFailedEvent
  | SagaStepExecutionFailedEvent;

/**
 * Orchestrates the reverse-order compensation workflow for the three doc-v3 §6.2 scenarios.
 *
 * Responsibilities:
 *   - Classify which scenario applies based on saga.status at failure time.
 *   - Dispatch the first compensating command with a durable COMPENSATION_STEP checkpoint.
 *   - Handle compensation ack events (BalanceReversed, LedgerReversed) and sequence the next step.
 *   - Transition the saga to REVERSED and then CLOSED when all compensation completes.
 *
 * This class does NOT set or read Payment Service state, and does NOT blur the
 * distinction between saga state and payment state (doc-v3 §7.5 and §7.6).
 *
 * doc-v3 §6.2 scenario reachability note:
 *   Scenario 3 (saga at BALANCE_RESERVED) is structurally complete and tested.
 *   Its production trigger (a post-balance-reserved step failing permanently)
 *   does not exist in Commit 11. It will be wired automatically when NotifyMerchant
 *   handling is implemented in a later commit — no coordinator changes will be needed.
 */
@Injectable()
export class CompensationCoordinator {
  constructor(
    private readonly sagaRepository: SagaRepository,
    private readonly commandDispatcher: CommandDispatcher,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('CompensationCoordinator');
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Classifies the compensation scenario based on saga status at failure time.
   * Called before initiateCompensation() to log the scenario explicitly.
   */
  classifyScenario(saga: SagaInstanceEntity): CompensationScenario {
    switch (saga.status) {
      case SagaStatus.ELIGIBILITY_PENDING:
        return 'SCENARIO_1';
      case SagaStatus.BALANCE_PENDING:
        return 'SCENARIO_2';
      case SagaStatus.BALANCE_RESERVED:
        return 'SCENARIO_3';
      default:
        return 'NONE';
    }
  }

  /**
   * Entry point for compensation. Called by SagaService after persisting failure metadata.
   *
   * Dispatches the first compensating command and persists a durable COMPENSATION_STEP
   * checkpoint. The checkpoint allows Commit 12's crash-recovery scanner to determine
   * how far compensation progressed if the process dies mid-sequence.
   *
   * Dispatching semantics:
   *   SCENARIO_1, SCENARIO_2 → dispatch ReverseLedgerEntry immediately
   *   SCENARIO_3 → dispatch ReverseBalance first; ReverseLedgerEntry follows BalanceReversed ack
   *   NONE → log and return; no commands dispatched
   */
  async initiateCompensation(
    saga: SagaInstanceEntity,
    event: TriggeringEvent
  ): Promise<void> {
    const scenario = this.classifyScenario(saga);

    const logContext = {
      sagaId: saga.id,
      paymentId: saga.paymentId,
      correlationId: saga.correlationId,
      failureReason: saga.failureReason,
      sagaStatus: saga.status,
      scenario,
    };

    if (scenario === 'NONE') {
      this.logger.info(
        'Compensation not required: no financial operations completed before the failure.',
        logContext
      );
      return;
    }

    this.logger.info(`Initiating compensation for §6.2 ${scenario}`, logContext);

    if (scenario === 'SCENARIO_3') {
      // doc-v3 §6.2 Scenario 3: Balance was reserved. Reverse balance first.
      await this.dispatchReverseBalance(saga, event.eventId);
    } else {
      // doc-v3 §6.2 Scenarios 1 and 2: Only the ledger entry completed. Reverse it directly.
      await this.dispatchReverseLedgerEntry(saga, event.eventId);
    }
  }

  /**
   * Handles the BalanceReversed acknowledgement for Scenario 3.
   * Called by SagaService when it receives a BalanceReversed event for a saga in compensation.
   *
   * Validates the event is expected, persists the BALANCE_REVERSAL_ACKNOWLEDGED checkpoint,
   * then dispatches ReverseLedgerEntry and persists LEDGER_REVERSAL_DISPATCHED.
   */
  async handleBalanceReversedAck(
    saga: SagaInstanceEntity,
    event: BalanceReversedEvent
  ): Promise<void> {
    const logContext = {
      sagaId: saga.id,
      paymentId: saga.paymentId,
      correlationId: saga.correlationId,
      sagaStatus: saga.status,
      eventId: event.eventId,
    };

    // Idempotency: if already REVERSED or CLOSED, a duplicate ack arrived — skip
    if (saga.status === SagaStatus.REVERSED || saga.status === SagaStatus.CLOSED) {
      this.logger.info('Duplicate BalanceReversed ack received for already-terminal saga. Skipping.', logContext);
      return;
    }

    // Validate a BALANCE_REVERSAL_DISPATCHED checkpoint exists (proves we initiated Scenario 3)
    const hasDispatched = await this.hasCompensationStep(
      saga.id,
      COMPENSATION_STEP_LABELS.BALANCE_REVERSAL_DISPATCHED
    );
    if (!hasDispatched) {
      this.logger.warn(
        'BalanceReversed received but no BALANCE_REVERSAL_DISPATCHED checkpoint found for this saga. Unexpected delivery — skipping.',
        logContext
      );
      return;
    }

    this.logger.info('BalanceReversed ack received for Scenario 3. Persisting checkpoint and dispatching ReverseLedgerEntry.', logContext);

    // Persist BALANCE_REVERSAL_ACKNOWLEDGED checkpoint
    const updatedSaga = await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        fromState: saga.status,
        toState: COMPENSATION_STEP_LABELS.BALANCE_REVERSAL_ACKNOWLEDGED,
        eventId: event.eventId,
        causationId: event.causationId || event.eventId,
        eventType: event.eventType,
      },
    ]);

    // Now dispatch ReverseLedgerEntry (second compensation step in Scenario 3)
    await this.dispatchReverseLedgerEntry(updatedSaga, event.eventId);
  }

  /**
   * Handles the LedgerReversed acknowledgement for all three scenarios.
   * Called by SagaService when it receives a LedgerReversed event for a saga in compensation.
   *
   * Transitions the saga from its current state → REVERSED → CLOSED, persisting each transition.
   */
  async handleLedgerReversedAck(
    saga: SagaInstanceEntity,
    event: LedgerReversedEvent
  ): Promise<void> {
    const logContext = {
      sagaId: saga.id,
      paymentId: saga.paymentId,
      correlationId: saga.correlationId,
      sagaStatus: saga.status,
      eventId: event.eventId,
    };

    // Idempotency: saga already at REVERSED or CLOSED — duplicate ack, skip
    if (saga.status === SagaStatus.REVERSED || saga.status === SagaStatus.CLOSED) {
      this.logger.info('Duplicate LedgerReversed ack received for already-terminal saga. Skipping.', logContext);
      return;
    }

    this.logger.info('LedgerReversed ack received. Transitioning saga to REVERSED then CLOSED.', logContext);

    const fromStatus = saga.status;

    // Transition 1: current state → REVERSED
    saga.transitionTo(SagaStatus.REVERSED);
    // Transition 2: REVERSED → CLOSED
    saga.transitionTo(SagaStatus.CLOSED);

    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.SAGA_STATUS,
        fromState: fromStatus,
        toState: SagaStatus.REVERSED,
        eventId: event.eventId,
        causationId: event.causationId || event.eventId,
        eventType: event.eventType,
      },
      {
        transitionType: SagaTransitionType.SAGA_STATUS,
        fromState: SagaStatus.REVERSED,
        toState: SagaStatus.CLOSED,
        eventId: event.eventId,
        causationId: event.causationId || event.eventId,
        eventType: event.eventType,
      },
    ]);

    this.logger.info('Saga compensation complete. Saga is now CLOSED.', {
      sagaId: saga.id,
      correlationId: saga.correlationId,
      completedAt: saga.completedAt?.toISOString(),
    });
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Dispatches a ReverseBalance command and persists the BALANCE_REVERSAL_DISPATCHED checkpoint.
   * Only called for Scenario 3.
   */
  private async dispatchReverseBalance(
    saga: SagaInstanceEntity,
    causationId: string
  ): Promise<void> {
    const commandId = randomUUID();

    // Persist checkpoint before dispatch — ensures the coordinator can recover if
    // the process dies between dispatch and Inbox PROCESSED on the balance service.
    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        fromState: saga.status,
        toState: COMPENSATION_STEP_LABELS.BALANCE_REVERSAL_DISPATCHED,
        eventId: commandId,
        causationId,
        eventType: REVERSE_BALANCE,
      },
    ]);

    await this.commandDispatcher.dispatch({
      eventId: commandId,
      eventType: REVERSE_BALANCE,
      correlationId: saga.correlationId,
      causationId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId: saga.paymentId,
        merchantId: saga.merchantId,
        amount: saga.amount,
        currency: saga.currency,
        reason: saga.failureReason || 'Saga compensation reversal',
      },
    });

    this.logger.info('ReverseBalance command dispatched for Scenario 3 compensation', {
      sagaId: saga.id,
      correlationId: saga.correlationId,
      commandId,
    });
  }

  /**
   * Dispatches a ReverseLedgerEntry command and persists the LEDGER_REVERSAL_DISPATCHED checkpoint.
   * Called directly for Scenarios 1 and 2, or via handleBalanceReversedAck for Scenario 3.
   */
  private async dispatchReverseLedgerEntry(
    saga: SagaInstanceEntity,
    causationId: string
  ): Promise<void> {
    const commandId = randomUUID();

    // Persist checkpoint before dispatch
    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        fromState: saga.status,
        toState: COMPENSATION_STEP_LABELS.LEDGER_REVERSAL_DISPATCHED,
        eventId: commandId,
        causationId,
        eventType: REVERSE_LEDGER_ENTRY,
      },
    ]);

    await this.commandDispatcher.dispatch({
      eventId: commandId,
      eventType: REVERSE_LEDGER_ENTRY,
      correlationId: saga.correlationId,
      causationId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId: saga.paymentId,
        merchantId: saga.merchantId,
        amount: saga.amount,
        currency: saga.currency,
        reason: saga.failureReason || 'Saga compensation reversal',
      },
    });

    this.logger.info('ReverseLedgerEntry command dispatched', {
      sagaId: saga.id,
      correlationId: saga.correlationId,
      commandId,
    });
  }

  /**
   * Checks whether a specific COMPENSATION_STEP transition label exists in the saga's history.
   * Used by handleBalanceReversedAck to validate the delivery is expected.
   */
  private async hasCompensationStep(sagaId: string, stepLabel: string): Promise<boolean> {
    return this.sagaRepository.hasCompensationStep(sagaId, stepLabel);
  }
}
