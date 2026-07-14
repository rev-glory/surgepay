import { Injectable, Optional } from '@nestjs/common';

import {
  BaseKafkaConsumer,
  KafkaEventProducer,
  LoggerService,
  MetricsService,
  TOPIC_REGISTRY,
} from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import { BaseEventEnvelope, PAYMENT_COMPLETED, PaymentCompletedEvent } from '@surgepay/events';

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
    // Pass metrics to the base consumer class to support observability
    super(config, logger, eventProducer, metrics);
  }

  protected async handleEvent(envelope: BaseEventEnvelope<unknown>): Promise<void> {
    const event = envelope as PaymentCompletedEvent;

    this.logger.info('Processing PaymentCompleted event inside Saga Orchestrator boundary', {
      eventId: event.eventId,
      eventType: event.eventType,
      paymentId: event.payload.paymentId,
      correlationId: event.correlationId,
      sagaId: event.sagaId,
      causationId: event.causationId,
    });

    await this.sagaService.processPaymentCompleted(event);
  }
}
