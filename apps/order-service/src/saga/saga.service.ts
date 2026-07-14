import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import type { PaymentCompletedEvent } from '@surgepay/events';

@Injectable()
export class SagaService {
  constructor(private readonly logger: LoggerService) {
    this.logger.setContext('SagaService');
  }

  /**
   * Safe entry boundary for processing a completed payment event.
   * This registers the event reception and logs the correlation and causation details,
   * establishing the workflow engine foundation for future steps.
   */
  async processPaymentCompleted(event: PaymentCompletedEvent): Promise<void> {
    this.logger.info('Saga Orchestrator entry point reached for PaymentCompleted event', {
      eventId: event.eventId,
      eventType: event.eventType,
      paymentId: event.payload.paymentId,
      orderId: event.payload.orderId,
      correlationId: event.correlationId,
      sagaId: event.sagaId,
      causationId: event.causationId,
    });
    
    // Future commits will implement SagaInstance creation, state transitions,
    // and downstream command publishing (RecordLedgerEntry, etc.).
  }
}
