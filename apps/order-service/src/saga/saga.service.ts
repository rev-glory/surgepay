import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import type { PaymentCompletedEvent } from '@surgepay/events';

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
      await this.sagaRepository.create(saga);
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
}
