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
  ELIGIBILITY_APPROVED,
  ELIGIBILITY_DENIED,
  type EligibilityApprovedEvent,
  type EligibilityDeniedEvent,
} from '@surgepay/events';

import { OrderInboxRepository } from '../../repositories/inbox.repository';
import { SagaService } from '../saga.service';

@Injectable()
export class SagaRiskEventsConsumer extends BaseKafkaConsumer {
  protected readonly topic = 'risk.events';
  protected readonly groupId = `${this.config.kafka.consumerGroupId}-saga-risk`;

  constructor(
    config: ConfigService,
    logger: LoggerService,
    protected readonly inboxRepository: OrderInboxRepository,
    eventProducer: KafkaEventProducer,
    private readonly sagaService: SagaService,
    @Optional() protected readonly metrics?: MetricsService
  ) {
    super(config, logger, eventProducer, metrics);
  }

  protected async handleEvent(envelope: BaseEventEnvelope<unknown>): Promise<void> {
    const { eventType, eventId } = envelope;

    if (eventType !== ELIGIBILITY_APPROVED && eventType !== ELIGIBILITY_DENIED) {
      this.logger.info(`Ignoring non-saga risk event type: ${eventType}`, {
        eventId,
        eventType,
      });
      return;
    }

    if (eventType === ELIGIBILITY_APPROVED) {
      const event = envelope as EligibilityApprovedEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.paymentId !== 'string' ||
        !payload.paymentId.trim()
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing EligibilityApproved event payload properties'
        );
      }

      this.logger.info('Processing EligibilityApproved event inside Saga Orchestrator', {
        eventId: event.eventId,
        eventType: event.eventType,
        paymentId: payload.paymentId,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
      });

      await this.sagaService.processEligibilityApproved(event);
    } else if (eventType === ELIGIBILITY_DENIED) {
      const event = envelope as EligibilityDeniedEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.paymentId !== 'string' ||
        !payload.paymentId.trim() ||
        typeof payload.reason !== 'string'
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing EligibilityDenied event payload properties'
        );
      }

      this.logger.info('Processing EligibilityDenied event inside Saga Orchestrator', {
        eventId: event.eventId,
        eventType: event.eventType,
        paymentId: payload.paymentId,
        reason: payload.reason,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
      });

      await this.sagaService.processEligibilityDenied(event);
    }
  }
}
