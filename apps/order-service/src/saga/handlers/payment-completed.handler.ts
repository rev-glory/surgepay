import { Injectable, Optional } from '@nestjs/common';

import {
  BaseKafkaConsumer,
  KafkaEventProducer,
  LoggerService,
  MalformedEventEnvelopeException,
  MetricsService,
  TOPIC_REGISTRY,
} from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import { BaseEventEnvelope, PAYMENT_COMPLETED, type PaymentCompletedEvent } from '@surgepay/events';

import { OrderInboxRepository } from '../../repositories/inbox.repository';
import { SagaService } from '../saga.service';

@Injectable()
export class SagaPaymentCompletedConsumer extends BaseKafkaConsumer {
  protected readonly topic = TOPIC_REGISTRY[PAYMENT_COMPLETED] || 'payments.completed';
  protected readonly groupId = `${this.config.kafka.consumerGroupId}-saga`;

  constructor(
    config: ConfigService,
    logger: LoggerService,
    protected readonly inboxRepository: OrderInboxRepository,
    eventProducer: KafkaEventProducer,
    private readonly sagaService: SagaService,
    @Optional() protected readonly metrics?: MetricsService,
  ) {
    super(config, logger, eventProducer, metrics);
  }

  protected async handleEvent(envelope: BaseEventEnvelope<unknown>): Promise<void> {
    const { eventType, eventId } = envelope;

    // Filter valid non-owned events cleanly (commits offset)
    if (eventType !== PAYMENT_COMPLETED) {
      this.logger.info(`Ignoring non-owned event type inside Saga Orchestrator consumer`, {
        eventId,
        eventType,
      });
      return;
    }

    // Now we know it is PAYMENT_COMPLETED. Run strict validation on payload.
    const event = envelope as PaymentCompletedEvent;
    const payload = event.payload;

    if (!payload || typeof payload !== 'object') {
      throw new MalformedEventEnvelopeException('Event payload is missing or not a valid object');
    }

    const {
      paymentId,
      amount,
      currency,
      merchantId,
      orderId,
      processorTransactionId,
      completedAt,
    } = payload;

    if (
      typeof paymentId !== 'string' ||
      !paymentId.trim() ||
      typeof amount !== 'number' ||
      amount <= 0 ||
      typeof currency !== 'string' ||
      !currency.trim() ||
      typeof merchantId !== 'string' ||
      !merchantId.trim() ||
      typeof orderId !== 'string' ||
      !orderId.trim() ||
      typeof processorTransactionId !== 'string' ||
      !processorTransactionId.trim() ||
      typeof completedAt !== 'string' ||
      !completedAt.trim()
    ) {
      throw new MalformedEventEnvelopeException(
        'Invalid or missing PaymentCompleted event payload properties'
      );
    }

    this.logger.info('Processing PaymentCompleted event inside Saga Orchestrator boundary', {
      eventId: event.eventId,
      eventType: event.eventType,
      paymentId,
      correlationId: event.correlationId,
      sagaId: event.sagaId,
      causationId: event.causationId,
    });

    await this.sagaService.processPaymentCompleted(event);
  }
}
