import { Injectable, Optional } from '@nestjs/common';

import {
  BaseKafkaConsumer,
  KafkaEventProducer,
  LoggerService,
  MalformedEventEnvelopeException,
  MetricsService,
} from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import {
  BaseEventEnvelope,
  ORDER_ELIGIBILITY_CONFIRMED,
  ORDER_ELIGIBILITY_REJECTED,
  type OrderEligibilityConfirmedEvent,
  type OrderEligibilityRejectedEvent,
} from '@surgepay/events';

import { OrderInboxRepository } from '../../repositories/inbox.repository';
import { SagaService } from '../saga.service';

@Injectable()
export class SagaOrderEventsConsumer extends BaseKafkaConsumer {
  protected readonly topic = 'order.events';
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
    if (eventType !== ORDER_ELIGIBILITY_CONFIRMED && eventType !== ORDER_ELIGIBILITY_REJECTED) {
      this.logger.info(`Ignoring non-saga order event type: ${eventType}`, {
        eventId,
        eventType,
      });
      return;
    }

    if (eventType === ORDER_ELIGIBILITY_CONFIRMED) {
      const event = envelope as OrderEligibilityConfirmedEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.orderId !== 'string' ||
        !payload.orderId.trim()
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing OrderEligibilityConfirmed event payload properties'
        );
      }

      this.logger.info('Processing OrderEligibilityConfirmed event inside Saga Orchestrator', {
        eventId: event.eventId,
        eventType: event.eventType,
        orderId: payload.orderId,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
        causationId: event.causationId,
      });

      await this.sagaService.processOrderEligibilityConfirmed(event);
    } else if (eventType === ORDER_ELIGIBILITY_REJECTED) {
      const event = envelope as OrderEligibilityRejectedEvent;
      const payload = event.payload;

      if (!payload || typeof payload !== 'object' || !payload.reason) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing OrderEligibilityRejected event payload properties'
        );
      }

      this.logger.info('Processing OrderEligibilityRejected event inside Saga Orchestrator', {
        eventId: event.eventId,
        eventType: event.eventType,
        orderId: payload.orderId,
        reason: payload.reason,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
        causationId: event.causationId,
      });

      await this.sagaService.processOrderEligibilityRejected(event);
    }
  }
}
