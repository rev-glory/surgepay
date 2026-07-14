import { randomUUID } from 'crypto';
import { Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { LoggerService } from '@surgepay/common';
import {
  CHECK_PAYOUT_ELIGIBILITY,
  RECORD_LEDGER_ENTRY,
  RESERVE_BALANCE,
  REVERSE_BALANCE,
  REVERSE_LEDGER_ENTRY,
} from '@surgepay/events';

import { SagaStatus, SagaTransitionType } from '../../generated/client';
import { CompensationCoordinator } from '../compensation/compensation.coordinator';
import { CommandDispatcher } from '../dispatchers/command.dispatcher';
import { SagaRepository } from '../repositories/saga.repository';
import { SagaInstanceEntity } from '../entities/saga-instance.entity';

@Injectable()
export class SagaRecoveryService implements OnApplicationBootstrap {
  constructor(
    private readonly sagaRepository: SagaRepository,
    private readonly commandDispatcher: CommandDispatcher,
    private readonly compensationCoordinator: CompensationCoordinator,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('SagaRecoveryService');
  }

  async onApplicationBootstrap(): Promise<void> {
    this.logger.info('Saga Recovery starting...');
    try {
      await this.recoverIncompleteSagas();
    } catch (err) {
      this.logger.error('Error during startup saga recovery', err as Error);
    }
  }

  async recoverIncompleteSagas(): Promise<void> {
    const recoverableSagas = await this.sagaRepository.findRecoverableSagas();
    if (recoverableSagas.length === 0) {
      this.logger.info('No recoverable sagas found.');
      return;
    }

    this.logger.info(`Found ${recoverableSagas.length} recoverable sagas.`);

    for (const saga of recoverableSagas) {
      try {
        await this.recoverSaga(saga);
      } catch (err) {
        this.logger.error(`Failed to recover saga ${saga.id}`, err as Error, {
          sagaId: saga.id,
          correlationId: saga.correlationId,
        });
        // Isolation: one saga recovery failure should not halt the recovery of others
      }
    }
  }

  private async recoverSaga(saga: SagaInstanceEntity): Promise<void> {
    const logContext = {
      sagaId: saga.id,
      correlationId: saga.correlationId,
      sagaStatus: saga.status,
      retryCount: saga.retryCount,
      nextRetryAt: saga.nextRetryAt,
      retryHandoffAt: saga.retryHandoffAt,
      failureReason: saga.failureReason,
    };

    // 1. Skip if owned by the Retry Scheduler
    if (saga.retryHandoffAt !== null || saga.nextRetryAt !== null) {
      this.logger.info(`Skipping recovery for saga: active Retry Scheduler custody.`, logContext);
      return;
    }

    this.logger.info(`Attempting recovery for saga.`, logContext);

    // 2. Compensation Flow Recovery (failureReason is set)
    if (saga.failureReason !== null) {
      await this.recoverCompensationFlow(saga);
      return;
    }

    // 3. Forward Flow Recovery
    await this.recoverForwardFlow(saga);
  }

  private async recoverCompensationFlow(saga: SagaInstanceEntity): Promise<void> {
    const logContext = { sagaId: saga.id, correlationId: saga.correlationId };

    // Convergence checks:
    // Rule 1: Saga status is REVERSED -> transition to CLOSED.
    if (saga.status === SagaStatus.REVERSED) {
      this.logger.info('Saga status is REVERSED. Transitioning to CLOSED.', logContext);
      saga.transitionTo(SagaStatus.CLOSED);
      saga.recordRecovery('Completed compensation flow closed');
      await this.sagaRepository.update(saga);
      return;
    }

    // Rule 2: LEDGER_REVERSAL_DISPATCHED exists (but not status REVERSED) -> redispatch
    const ledgerDispatched = await this.sagaRepository.findCompensationStep(
      saga.id,
      'LEDGER_REVERSAL_DISPATCHED'
    );
    if (ledgerDispatched) {
      this.logger.info('Resuming compensation: redispatching ReverseLedgerEntry.', {
        ...logContext,
        commandId: ledgerDispatched.eventId,
      });

      // Update recovery metadata first
      saga.recordRecovery('Resumed and redispatched ReverseLedgerEntry');
      await this.sagaRepository.update(saga);

      await this.commandDispatcher.dispatch({
        eventId: ledgerDispatched.eventId,
        eventType: REVERSE_LEDGER_ENTRY,
        correlationId: saga.correlationId,
        causationId: ledgerDispatched.eventId,
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
      return;
    }

    // Rule 3: BALANCE_REVERSAL_ACKNOWLEDGED exists (but not LEDGER_REVERSAL_DISPATCHED) -> dispatch
    const balanceAcked = await this.sagaRepository.findCompensationStep(
      saga.id,
      'BALANCE_REVERSAL_ACKNOWLEDGED'
    );
    if (balanceAcked) {
      this.logger.info('Resuming compensation: dispatching ReverseLedgerEntry.', logContext);
      const commandId = randomUUID();

      saga.recordRecovery('Resumed and dispatched ReverseLedgerEntry');
      // Update DB with checkpoint and metadata in a single transaction
      await this.sagaRepository.update(saga, [
        {
          transitionType: SagaTransitionType.COMPENSATION_STEP,
          fromState: saga.status,
          toState: 'LEDGER_REVERSAL_DISPATCHED',
          eventId: commandId,
          causationId: balanceAcked.eventId,
          eventType: REVERSE_LEDGER_ENTRY,
        },
      ]);

      await this.commandDispatcher.dispatch({
        eventId: commandId,
        eventType: REVERSE_LEDGER_ENTRY,
        correlationId: saga.correlationId,
        causationId: balanceAcked.eventId,
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
      return;
    }

    // Rule 4: BALANCE_REVERSAL_DISPATCHED exists (but not BALANCE_REVERSAL_ACKNOWLEDGED) -> redispatch
    const balanceDispatched = await this.sagaRepository.findCompensationStep(
      saga.id,
      'BALANCE_REVERSAL_DISPATCHED'
    );
    if (balanceDispatched) {
      this.logger.info('Resuming compensation: redispatching ReverseBalance.', {
        ...logContext,
        commandId: balanceDispatched.eventId,
      });

      saga.recordRecovery('Resumed and redispatched ReverseBalance');
      await this.sagaRepository.update(saga);

      await this.commandDispatcher.dispatch({
        eventId: balanceDispatched.eventId,
        eventType: REVERSE_BALANCE,
        correlationId: saga.correlationId,
        causationId: balanceDispatched.eventId,
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
      return;
    }

    // Rule 5: None of the above (compensation trigger crashed before dispatching) -> initiate compensation
    this.logger.info('Resuming compensation: initiating compensation scenario.', logContext);
    saga.recordRecovery('Initiated compensation sequence from crash state');
    await this.sagaRepository.update(saga);

    await this.compensationCoordinator.initiateCompensation(saga, {
      eventId: saga.correlationId,
      eventType: 'SagaRecoveryTrigger',
      correlationId: saga.correlationId,
      causationId: saga.correlationId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId: saga.paymentId,
        failureReason: saga.failureReason || 'Saga recovery retry',
      },
    } as any);
  }

  private async recoverForwardFlow(saga: SagaInstanceEntity): Promise<void> {
    const logContext = { sagaId: saga.id, correlationId: saga.correlationId };

    switch (saga.status) {
      case SagaStatus.LEDGER_PENDING:
        if (saga.orderValidationStatus === 'REJECTED') {
          this.logger.info('LEDGER_PENDING forward flow: order validation is REJECTED. Closing saga.', logContext);
          saga.transitionTo(SagaStatus.CLOSED);
          saga.recordRecovery('Closed due to order rejection');
          await this.sagaRepository.update(saga);
        } else {
          this.logger.info('LEDGER_PENDING forward flow: redispatching RecordLedgerEntry.', {
            ...logContext,
            commandId: saga.currentCommandId,
          });
          if (!saga.currentCommandId) {
            throw new Error(`Saga ${saga.id} is in LEDGER_PENDING but lacks currentCommandId`);
          }

          saga.recordRecovery('Redispatched RecordLedgerEntry');
          await this.sagaRepository.update(saga);

          await this.commandDispatcher.dispatch({
            eventId: saga.currentCommandId,
            eventType: RECORD_LEDGER_ENTRY,
            correlationId: saga.correlationId,
            causationId: saga.correlationId,
            sagaId: saga.id,
            timestamp: new Date().toISOString(),
            version: 1,
            payload: {
              paymentId: saga.paymentId,
              merchantId: saga.merchantId,
              amount: saga.amount,
              currency: saga.currency,
              entryType: 'DEBIT',
              description: `Payment ledger record for payment ${saga.paymentId}`,
            },
          });
        }
        break;

      case SagaStatus.LEDGER_RECORDED: {
        this.logger.info('LEDGER_RECORDED forward flow: advancing to ELIGIBILITY_PENDING.', logContext);
        const checkPayoutCommandId = randomUUID();
        saga.transitionTo(SagaStatus.ELIGIBILITY_PENDING);
        saga.resetRetryMetadata(checkPayoutCommandId);
        saga.recordRecovery('Advanced to ELIGIBILITY_PENDING');

        await this.sagaRepository.update(saga, [
          {
            transitionType: SagaTransitionType.SAGA_STATUS,
            fromState: SagaStatus.LEDGER_RECORDED,
            toState: SagaStatus.ELIGIBILITY_PENDING,
            eventId: checkPayoutCommandId,
            causationId: saga.correlationId,
            eventType: CHECK_PAYOUT_ELIGIBILITY,
          },
        ]);

        await this.commandDispatcher.dispatch({
          eventId: checkPayoutCommandId,
          eventType: CHECK_PAYOUT_ELIGIBILITY,
          correlationId: saga.correlationId,
          causationId: saga.correlationId,
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
        break;
      }

      case SagaStatus.ELIGIBILITY_PENDING:
        this.logger.info('ELIGIBILITY_PENDING forward flow: redispatching CheckPayoutEligibility.', {
          ...logContext,
          commandId: saga.currentCommandId,
        });
        if (!saga.currentCommandId) {
          throw new Error(`Saga ${saga.id} is in ELIGIBILITY_PENDING but lacks currentCommandId`);
        }

        saga.recordRecovery('Redispatched CheckPayoutEligibility');
        await this.sagaRepository.update(saga);

        await this.commandDispatcher.dispatch({
          eventId: saga.currentCommandId,
          eventType: CHECK_PAYOUT_ELIGIBILITY,
          correlationId: saga.correlationId,
          causationId: saga.correlationId,
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
        break;

      case SagaStatus.BALANCE_PENDING:
        this.logger.info('BALANCE_PENDING forward flow: redispatching ReserveBalance.', {
          ...logContext,
          commandId: saga.currentCommandId,
        });
        if (!saga.currentCommandId) {
          throw new Error(`Saga ${saga.id} is in BALANCE_PENDING but lacks currentCommandId`);
        }

        saga.recordRecovery('Redispatched ReserveBalance');
        await this.sagaRepository.update(saga);

        await this.commandDispatcher.dispatch({
          eventId: saga.currentCommandId,
          eventType: RESERVE_BALANCE,
          correlationId: saga.correlationId,
          causationId: saga.correlationId,
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
        break;

      case SagaStatus.BALANCE_RESERVED:
        this.logger.info('BALANCE_RESERVED forward flow: reached Day 5 flow boundary. Skipping.', logContext);
        break;

      default:
        this.logger.warn(`Saga status recovery not supported or unhandled: ${saga.status}`, logContext);
        break;
    }
  }
}
