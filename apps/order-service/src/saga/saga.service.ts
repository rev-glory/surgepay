import { randomUUID } from 'crypto';

import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import {
  type BalanceReservationFailedEvent,
  type BalanceReservedEvent,
  type BalanceReversedEvent,
  CHECK_ORDER_ELIGIBILITY,
  CHECK_PAYOUT_ELIGIBILITY,
  type EligibilityApprovedEvent,
  type EligibilityDeniedEvent,
  type LedgerEntryRecordedEvent,
  type LedgerRecordingFailedEvent,
  type LedgerReversedEvent,
  type OrderEligibilityConfirmedEvent,
  type OrderEligibilityRejectedEvent,
  type PaymentCompletedEvent,
  RECORD_LEDGER_ENTRY,
  RESERVE_BALANCE,
  type SagaRetryRegisteredEvent,
  type SagaStepExecutionFailedEvent,
} from '@surgepay/events';

import {
  OrderValidationStatus,
  SagaStatus,
  SagaTransitionType,
} from '../generated/client';
import { CompensationCoordinator } from './compensation/compensation.coordinator';
import { CommandDispatcher } from './dispatchers/command.dispatcher';
import { SagaInstanceEntity } from './entities/saga-instance.entity';
import { SagaRepository } from './repositories/saga.repository';

@Injectable()
export class SagaService {
  constructor(
    private readonly logger: LoggerService,
    private readonly sagaRepository: SagaRepository,
    private readonly commandDispatcher: CommandDispatcher,
    private readonly compensationCoordinator: CompensationCoordinator
  ) {
    this.logger.setContext('SagaService');
  }

  /**
   * Safe entry boundary for processing a completed payment event.
   * This registers the event reception, loads or initializes a durable SagaInstance,
   * and persists the initial LEDGER_PENDING state.
   */
  async processPaymentCompleted(event: PaymentCompletedEvent): Promise<void> {
    const { correlationId } = event;
    const { paymentId, orderId, merchantId, amount, currency } = event.payload;

    this.logger.info('Saga Orchestrator entry point reached for PaymentCompleted event', {
      eventId: event.eventId,
      eventType: event.eventType,
      paymentId,
      orderId,
      correlationId,
      sagaId: event.sagaId,
      causationId: event.causationId,
    });

    // Check for existing saga instance to prevent duplicate workflow runs
    const existing = await this.sagaRepository.findByPaymentId(paymentId);
    if (existing) {
      this.logger.warn('SagaInstance already exists for payment. Skipping initialization.', {
        paymentId,
        sagaId: existing.id,
        correlationId,
      });
      return;
    }

    const ledgerCommandId = randomUUID();

    // Create SagaInstance aggregate root. Saga ID (id) adopts correlationId as per doc-v3 Section 8.4
    const saga = SagaInstanceEntity.create({
      paymentId,
      correlationId,
      merchantId,
      amount,
      currency,
      initialCommandId: ledgerCommandId,
    });

    // Persist new saga instance to the database
    try {
      await this.sagaRepository.create(saga, [
        {
          transitionType: SagaTransitionType.SAGA_STATUS,
          fromState: 'NONE',
          toState: SagaStatus.LEDGER_PENDING,
          eventId: event.eventId,
          causationId: event.causationId || event.eventId,
          eventType: event.eventType,
        },
        {
          transitionType: SagaTransitionType.ORDER_VALIDATION,
          fromState: 'NONE',
          toState: OrderValidationStatus.PENDING,
          eventId: event.eventId,
          causationId: event.causationId || event.eventId,
          eventType: event.eventType,
        },
      ]);
    } catch (err: unknown) {
      const error = err as { code?: string; meta?: { target?: string[] } };
      if (error && error.code === 'P2002') {
        const targets = error.meta?.target || [];
        if (targets.includes('paymentId')) {
          const collidedSaga = await this.sagaRepository.findByPaymentId(paymentId);
          if (collidedSaga && collidedSaga.correlationId === correlationId) {
            this.logger.warn('Duplicate SagaInstance insert race detected. Safe idempotent skip.', {
              paymentId: collidedSaga.paymentId,
              sagaId: collidedSaga.id,
              correlationId,
            });
            return;
          }
        }
      }
      throw err;
    }

    this.logger.info('Durable SagaInstance created successfully in LEDGER_PENDING state', {
      sagaId: saga.id,
      paymentId: saga.paymentId,
      correlationId: saga.correlationId,
      sagaStatus: saga.status,
    });

    // Dispatch CheckOrderEligibility command
    await this.commandDispatcher.dispatch({
      eventId: randomUUID(),
      eventType: CHECK_ORDER_ELIGIBILITY,
      correlationId,
      causationId: event.eventId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        orderId,
        paymentId,
        merchantId,
        amount,
        currency,
      },
    });

    // Dispatch RecordLedgerEntry command
    await this.commandDispatcher.dispatch({
      eventId: ledgerCommandId,
      eventType: RECORD_LEDGER_ENTRY,
      correlationId,
      causationId: event.eventId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId,
        merchantId,
        amount,
        currency,
        entryType: 'DEBIT',
        description: `Payment ledger record for payment ${paymentId}`,
      },
    });
  }

  /**
   * Processes order eligibility confirmation response event.
   */
  async processOrderEligibilityConfirmed(event: OrderEligibilityConfirmedEvent): Promise<void> {
    const { correlationId, sagaId, eventId, eventType, causationId } = event;
    const { orderId } = event.payload;

    this.logger.info('Processing OrderEligibilityConfirmed inside Saga Service', {
      eventId,
      eventType,
      correlationId,
      sagaId,
      orderId,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for OrderEligibilityConfirmed event', {
        sagaId,
        correlationId,
        eventId,
      });
      return;
    }

    if (saga.orderValidationStatus === OrderValidationStatus.CONFIRMED) {
      this.logger.info('Order validation already CONFIRMED. Safe skip.', {
        sagaId: saga.id,
        correlationId: saga.correlationId,
      });
      return;
    }

    const fromState = saga.orderValidationStatus;
    saga.confirmOrder();

    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.ORDER_VALIDATION,
        fromState,
        toState: saga.orderValidationStatus,
        eventId,
        causationId: causationId || eventId,
        eventType,
      },
    ]);

    this.logger.info('Saga order validation status updated to CONFIRMED. Financial status remains LEDGER_PENDING.', {
      sagaId: saga.id,
      correlationId: saga.correlationId,
      version: saga.version,
    });
  }

  /**
   * Processes order eligibility rejection response event.
   */
  async processOrderEligibilityRejected(event: OrderEligibilityRejectedEvent): Promise<void> {
    const { correlationId, sagaId, eventId, eventType, causationId } = event;
    const { orderId, reason } = event.payload;

    this.logger.info('Processing OrderEligibilityRejected inside Saga Service', {
      eventId,
      eventType,
      correlationId,
      sagaId,
      orderId,
      reason,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for OrderEligibilityRejected event', {
        sagaId,
        correlationId,
        eventId,
      });
      return;
    }

    if (saga.orderValidationStatus === OrderValidationStatus.REJECTED) {
      this.logger.info('Order validation already REJECTED. Safe skip.', {
        sagaId: saga.id,
        correlationId: saga.correlationId,
      });
      return;
    }

    const fromState = saga.orderValidationStatus;
    saga.rejectOrder(`Order eligibility check failed: ${reason}`, 'order-service');

    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.ORDER_VALIDATION,
        fromState,
        toState: saga.orderValidationStatus,
        eventId,
        causationId: causationId || eventId,
        eventType,
      },
    ]);

    this.logger.info('Saga order validation status updated to REJECTED. Financial status remains LEDGER_PENDING.', {
      sagaId: saga.id,
      correlationId: saga.correlationId,
      version: saga.version,
      failureReason: saga.failureReason,
    });
  }

  /**
   * Processes LedgerEntryRecorded response event.
   */
  async processLedgerEntryRecorded(event: LedgerEntryRecordedEvent): Promise<void> {
    const { correlationId, sagaId, eventId, eventType, causationId } = event;
    const { paymentId } = event.payload;

    this.logger.info('Processing LedgerEntryRecorded inside Saga Service', {
      eventId,
      eventType,
      correlationId,
      sagaId,
      paymentId,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for LedgerEntryRecorded event', {
        sagaId,
        correlationId,
        eventId,
      });
      return;
    }

    if (saga.isTerminal()) {
      this.logger.info('Saga is already terminal. Safe skip.', {
        sagaId: saga.id,
        sagaStatus: saga.status,
      });
      return;
    }

    // Duplicate skip guard: check if already reached or moved beyond
    const forwardChain: SagaStatus[] = [
      SagaStatus.LEDGER_PENDING,
      SagaStatus.LEDGER_RECORDED,
      SagaStatus.ELIGIBILITY_PENDING,
      SagaStatus.BALANCE_PENDING,
      SagaStatus.BALANCE_RESERVED,
    ];
    if (forwardChain.indexOf(saga.status) >= forwardChain.indexOf(SagaStatus.LEDGER_RECORDED)) {
      this.logger.info('LedgerEntryRecorded already applied. Safe skip.', {
        sagaId: saga.id,
        sagaStatus: saga.status,
      });
      return;
    }

    // Invariant check: cannot transition to LEDGER_RECORDED unless order validation is confirmed
    if (saga.orderValidationStatus !== OrderValidationStatus.CONFIRMED) {
      throw new Error(
        `State Conflict Guard: Cannot transition to LEDGER_RECORDED because order validation is ${saga.orderValidationStatus}`
      );
    }

    const eligibilityCommandId = randomUUID();
    const oldStatus = saga.status;
    saga.transitionTo(SagaStatus.LEDGER_RECORDED);
    saga.transitionTo(SagaStatus.ELIGIBILITY_PENDING);
    saga.resetRetryMetadata(eligibilityCommandId);

    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.SAGA_STATUS,
        fromState: oldStatus,
        toState: SagaStatus.LEDGER_RECORDED,
        eventId,
        causationId: causationId || eventId,
        eventType,
      },
      {
        transitionType: SagaTransitionType.SAGA_STATUS,
        fromState: SagaStatus.LEDGER_RECORDED,
        toState: SagaStatus.ELIGIBILITY_PENDING,
        eventId,
        causationId: causationId || eventId,
        eventType,
      },
    ]);

    this.logger.info('Saga transitioned to ELIGIBILITY_PENDING. Dispatching CheckPayoutEligibility.', {
      sagaId: saga.id,
      correlationId: saga.correlationId,
    });

    await this.commandDispatcher.dispatch({
      eventId: eligibilityCommandId,
      eventType: CHECK_PAYOUT_ELIGIBILITY,
      correlationId: saga.correlationId,
      causationId: eventId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId: saga.paymentId,
        merchantId: saga.merchantId,
        amount: saga.amount,
        currency: saga.currency,
      },
    });
  }

  /**
   * Processes LedgerRecordingFailed response event.
   */
  async processLedgerRecordingFailed(event: LedgerRecordingFailedEvent): Promise<void> {
    const { correlationId, sagaId, eventId, eventType } = event;
    const { paymentId, reason } = event.payload;

    this.logger.info('Processing LedgerRecordingFailed inside Saga Service', {
      eventId,
      eventType,
      correlationId,
      sagaId,
      paymentId,
      reason,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for LedgerRecordingFailed event', {
        sagaId,
        correlationId,
        eventId,
      });
      return;
    }

    if (saga.failureReason !== null) {
      this.logger.info('Saga already has failure metadata set. Safe skip.', {
        sagaId: saga.id,
      });
      return;
    }

    saga.failureReason = `Ledger recording failed: ${reason}`;
    saga.failedAt = new Date();
    saga.originService = 'ledger-service';

    await this.sagaRepository.update(saga);

    this.logger.warn('Saga forward execution stopped due to ledger recording failure', {
      sagaId: saga.id,
      failureReason: saga.failureReason,
    });
  }

  /**
   * Processes EligibilityApproved response event.
   */
  async processEligibilityApproved(event: EligibilityApprovedEvent): Promise<void> {
    const { correlationId, sagaId, eventId, eventType, causationId } = event;
    const { paymentId } = event.payload;

    this.logger.info('Processing EligibilityApproved inside Saga Service', {
      eventId,
      eventType,
      correlationId,
      sagaId,
      paymentId,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for EligibilityApproved event', {
        sagaId,
        correlationId,
        eventId,
      });
      return;
    }

    if (saga.isTerminal()) {
      this.logger.info('Saga is already terminal. Safe skip.', {
        sagaId: saga.id,
        sagaStatus: saga.status,
      });
      return;
    }

    const forwardChain: SagaStatus[] = [
      SagaStatus.LEDGER_PENDING,
      SagaStatus.LEDGER_RECORDED,
      SagaStatus.ELIGIBILITY_PENDING,
      SagaStatus.BALANCE_PENDING,
      SagaStatus.BALANCE_RESERVED,
    ];
    if (forwardChain.indexOf(saga.status) >= forwardChain.indexOf(SagaStatus.BALANCE_PENDING)) {
      this.logger.info('EligibilityApproved already processed. Safe skip.', {
        sagaId: saga.id,
        sagaStatus: saga.status,
      });
      return;
    }

    const balanceCommandId = randomUUID();
    const oldStatus = saga.status;
    saga.transitionTo(SagaStatus.BALANCE_PENDING);
    saga.resetRetryMetadata(balanceCommandId);

    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.SAGA_STATUS,
        fromState: oldStatus,
        toState: SagaStatus.BALANCE_PENDING,
        eventId,
        causationId: causationId || eventId,
        eventType,
      },
    ]);

    this.logger.info('Saga transitioned to BALANCE_PENDING. Dispatching ReserveBalance.', {
      sagaId: saga.id,
      correlationId: saga.correlationId,
    });

    await this.commandDispatcher.dispatch({
      eventId: balanceCommandId,
      eventType: RESERVE_BALANCE,
      correlationId: saga.correlationId,
      causationId: eventId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId: saga.paymentId,
        merchantId: saga.merchantId,
        amount: saga.amount,
        currency: saga.currency,
      },
    });
  }

  /**
   * Processes EligibilityDenied response event.
   * §6.2 Scenario 1: ledger entry recorded, eligibility denied → reverse ledger.
   */
  async processEligibilityDenied(event: EligibilityDeniedEvent): Promise<void> {
    const { correlationId, sagaId, eventId, eventType } = event;
    const { paymentId, reason } = event.payload;

    this.logger.info('Processing EligibilityDenied inside Saga Service', {
      eventId,
      eventType,
      correlationId,
      sagaId,
      paymentId,
      reason,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for EligibilityDenied event', {
        sagaId,
        correlationId,
        eventId,
      });
      return;
    }

    if (saga.failureReason !== null) {
      this.logger.info('Saga already has failure metadata set. Safe skip.', {
        sagaId: saga.id,
      });
      return;
    }

    saga.failureReason = `Eligibility denied: ${reason}`;
    saga.failedAt = new Date();
    saga.originService = 'risk-engine';

    const updatedSaga = await this.sagaRepository.update(saga);

    this.logger.warn('Saga forward execution stopped due to payout eligibility denial. Initiating compensation.', {
      sagaId: saga.id,
      failureReason: saga.failureReason,
    });

    // Initiate compensation — §6.2 Scenario 1 (only LedgerEntryRecorded completed)
    await this.compensationCoordinator.initiateCompensation(updatedSaga, event);
  }

  /**
   * Processes BalanceReserved response event.
   */
  async processBalanceReserved(event: BalanceReservedEvent): Promise<void> {
    const { correlationId, sagaId, eventId, eventType, causationId } = event;
    const { paymentId } = event.payload;

    this.logger.info('Processing BalanceReserved inside Saga Service', {
      eventId,
      eventType,
      correlationId,
      sagaId,
      paymentId,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for BalanceReserved event', {
        sagaId,
        correlationId,
        eventId,
      });
      return;
    }

    if (saga.isTerminal()) {
      this.logger.info('Saga is already terminal. Safe skip.', {
        sagaId: saga.id,
        sagaStatus: saga.status,
      });
      return;
    }

    if (saga.status === SagaStatus.BALANCE_RESERVED) {
      this.logger.info('BalanceReserved already processed. Safe skip.', {
        sagaId: saga.id,
      });
      return;
    }

    const oldStatus = saga.status;
    saga.transitionTo(SagaStatus.BALANCE_RESERVED);
    saga.resetRetryMetadata(null);

    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.SAGA_STATUS,
        fromState: oldStatus,
        toState: SagaStatus.BALANCE_RESERVED,
        eventId,
        causationId: causationId || eventId,
        eventType,
      },
    ]);

    this.logger.info('Saga status successfully reached BALANCE_RESERVED', {
      sagaId: saga.id,
      correlationId: saga.correlationId,
    });
  }

  /**
   * Processes BalanceReservationFailed response event.
   * §6.2 Scenario 2: ledger entry recorded, balance reservation permanently failed → reverse ledger.
   */
  async processBalanceReservationFailed(event: BalanceReservationFailedEvent): Promise<void> {
    const { correlationId, sagaId, eventId, eventType } = event;
    const { paymentId, reason } = event.payload;

    this.logger.info('Processing BalanceReservationFailed inside Saga Service', {
      eventId,
      eventType,
      correlationId,
      sagaId,
      paymentId,
      reason,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for BalanceReservationFailed event', {
        sagaId,
        correlationId,
        eventId,
      });
      return;
    }

    if (saga.failureReason !== null) {
      this.logger.info('Saga already has failure metadata set. Safe skip.', {
        sagaId: saga.id,
      });
      return;
    }

    saga.failureReason = `Balance reservation failed: ${reason}`;
    saga.failedAt = new Date();
    saga.originService = 'balance-service';

    const updatedSaga = await this.sagaRepository.update(saga);

    this.logger.warn('Saga forward execution stopped due to balance reservation failure. Initiating compensation.', {
      sagaId: saga.id,
      failureReason: saga.failureReason,
    });

    // Initiate compensation — §6.2 Scenario 2 (LedgerEntryRecorded completed; balance never reserved)
    await this.compensationCoordinator.initiateCompensation(updatedSaga, event);
  }

  /**
   * Processes the Saga-facing SagaRetryRegistered outcome event.
   * Clears handoff lock and updates observed retry metadata.
   */
  async processRetryRegistered(event: SagaRetryRegisteredEvent): Promise<void> {
    const { sagaId, eventId, eventType } = event;
    const { originalEventId, attempt, nextExecutionTime } = event.payload;

    this.logger.info('Processing SagaRetryRegistered event', {
      eventId,
      eventType,
      sagaId,
      originalEventId,
      attempt,
      nextExecutionTime,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for SagaRetryRegistered event', {
        sagaId,
        originalEventId,
      });
      return;
    }

    saga.registerRetry(attempt, new Date(nextExecutionTime));
    await this.sagaRepository.update(saga);

    this.logger.info('Saga retry execution successfully registered', {
      sagaId: saga.id,
      retryCount: saga.retryCount,
      nextRetryAt: saga.nextRetryAt?.toISOString(),
    });
  }

  /**
   * Processes the Saga-facing SagaStepExecutionFailed failure event.
   * Transitions Saga to step-failure, halting forward execution.
   * Initiates compensation based on how far the forward saga progressed:
   *   - ELIGIBILITY_PENDING → §6.2 Scenario 1
   *   - BALANCE_PENDING     → §6.2 Scenario 2
   *   - BALANCE_RESERVED    → §6.2 Scenario 3
   *   - LEDGER_PENDING      → No compensation (nothing committed)
   */
  async processStepExecutionFailed(event: SagaStepExecutionFailedEvent): Promise<void> {
    const { sagaId, eventId, eventType } = event;
    const { originalEventId, failureReason } = event.payload;

    this.logger.info('Processing SagaStepExecutionFailed event', {
      eventId,
      eventType,
      sagaId,
      originalEventId,
      failureReason,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for SagaStepExecutionFailed event', {
        sagaId,
        originalEventId,
      });
      return;
    }

    if (saga.failureReason !== null) {
      this.logger.info('Saga already has failure metadata set. Safe skip.', {
        sagaId: saga.id,
      });
      return;
    }

    saga.failStep(failureReason, 'retry-scheduler');
    const updatedSaga = await this.sagaRepository.update(saga);

    this.logger.warn('Saga forward execution halted permanently due to retry scheduler exhaustion. Initiating compensation.', {
      sagaId: saga.id,
      sagaStatus: saga.status,
      failureReason: saga.failureReason,
    });

    // Initiate compensation — scenario determined by current saga.status
    await this.compensationCoordinator.initiateCompensation(updatedSaga, event);
  }

  /**
   * Processes BalanceReversed event during compensation (§6.2 Scenario 3 only).
   * Delegates to CompensationCoordinator which validates the ack is expected,
   * persists the BALANCE_REVERSAL_ACKNOWLEDGED checkpoint, and dispatches ReverseLedgerEntry.
   */
  async processBalanceReversedForCompensation(event: BalanceReversedEvent): Promise<void> {
    const { sagaId, eventId, eventType, correlationId } = event;

    this.logger.info('Processing BalanceReversed event for saga compensation', {
      eventId,
      eventType,
      sagaId,
      correlationId,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for BalanceReversed event. Skipping.', {
        sagaId,
        eventId,
      });
      return;
    }

    await this.compensationCoordinator.handleBalanceReversedAck(saga, event);
  }

  /**
   * Processes LedgerReversed event during compensation (§6.2 Scenarios 1, 2, and 3).
   * Delegates to CompensationCoordinator which transitions the saga to REVERSED → CLOSED.
   */
  async processLedgerReversed(event: LedgerReversedEvent): Promise<void> {
    const { sagaId, eventId, eventType, correlationId } = event;

    this.logger.info('Processing LedgerReversed event for saga compensation', {
      eventId,
      eventType,
      sagaId,
      correlationId,
    });

    const saga = await this.sagaRepository.findById(sagaId);
    if (!saga) {
      this.logger.warn('SagaInstance not found for LedgerReversed event. Skipping.', {
        sagaId,
        eventId,
      });
      return;
    }

    await this.compensationCoordinator.handleLedgerReversedAck(saga, event);
  }
}
