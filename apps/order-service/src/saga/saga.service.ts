import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import {
  type PaymentCompletedEvent,
  type OrderEligibilityConfirmedEvent,
  type OrderEligibilityRejectedEvent,
} from '@surgepay/events';

import {
  SagaStatus,
  OrderValidationStatus,
  SagaTransitionType,
} from '../generated/client';
import { SagaInstanceEntity } from './entities/saga-instance.entity';
import { SagaRepository } from './repositories/saga.repository';

@Injectable()
export class SagaService {
  constructor(
    private readonly logger: LoggerService,
    private readonly sagaRepository: SagaRepository
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
    const { paymentId, orderId } = event.payload;

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

    // Create SagaInstance aggregate root. Saga ID (id) adopts correlationId as per doc-v3 Section 8.4
    const saga = SagaInstanceEntity.create({
      paymentId,
      correlationId,
    });

    // Persist new saga instance to the database
    try {
      await this.sagaRepository.create(saga, [
        {
          transitionType: SagaTransitionType.SAGA_STATUS,
          fromState: 'NONE',
          toState: SagaStatus.LEDGER_PENDING,
          eventId: event.eventId,
          causationId: event.causationId,
          eventType: event.eventType,
        },
        {
          transitionType: SagaTransitionType.ORDER_VALIDATION,
          fromState: 'NONE',
          toState: OrderValidationStatus.PENDING,
          eventId: event.eventId,
          causationId: event.causationId,
          eventType: event.eventType,
        },
      ]);
    } catch (err: unknown) {
      const error = err as { code?: string; meta?: { target?: string[] } };
      if (error && error.code === 'P2002') {
        const targets = error.meta?.target || [];
        if (targets.includes('paymentId')) {
          // Double-check the existing record to confirm it matches the same payment workflow
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
      // Re-throw if it is an unexpected collision or database error
      throw err;
    }

    this.logger.info('Durable SagaInstance created successfully in LEDGER_PENDING state', {
      sagaId: saga.id,
      paymentId: saga.paymentId,
      correlationId: saga.correlationId,
      sagaStatus: saga.status,
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

    const fromState = saga.orderValidationStatus;
    saga.confirmOrder();

    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.ORDER_VALIDATION,
        fromState,
        toState: saga.orderValidationStatus,
        eventId,
        causationId,
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

    const fromState = saga.orderValidationStatus;
    saga.rejectOrder(`Order eligibility check failed: ${reason}`, 'order-service');

    await this.sagaRepository.update(saga, [
      {
        transitionType: SagaTransitionType.ORDER_VALIDATION,
        fromState,
        toState: saga.orderValidationStatus,
        eventId,
        causationId,
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
}
