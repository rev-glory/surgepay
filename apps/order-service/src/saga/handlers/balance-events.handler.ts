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
  BALANCE_RESERVATION_FAILED,
  BALANCE_RESERVED,
  BALANCE_REVERSED,
  type BalanceReservationFailedEvent,
  type BalanceReservedEvent,
  type BalanceReversedEvent,
  BaseEventEnvelope,
} from '@surgepay/events';

import { OrderInboxRepository } from '../../repositories/inbox.repository';
import { SagaService } from '../saga.service';

@Injectable()
export class SagaBalanceEventsConsumer extends BaseKafkaConsumer {
  protected readonly topic = 'balance.events';
  protected readonly groupId = `${this.config.kafka.consumerGroupId}-saga-balance`;

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

    if (eventType === BALANCE_RESERVED) {
      const event = envelope as BalanceReservedEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.paymentId !== 'string' ||
        !payload.paymentId.trim()
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing BalanceReserved event payload properties'
        );
      }

      this.logger.info('Processing BalanceReserved event inside Saga Orchestrator', {
        eventId: event.eventId,
        eventType: event.eventType,
        paymentId: payload.paymentId,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
      });

      await this.sagaService.processBalanceReserved(event);
      return;
    }

    if (eventType === BALANCE_RESERVATION_FAILED) {
      const event = envelope as BalanceReservationFailedEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.paymentId !== 'string' ||
        !payload.paymentId.trim() ||
        typeof payload.reason !== 'string'
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing BalanceReservationFailed event payload properties'
        );
      }

      this.logger.info('Processing BalanceReservationFailed event inside Saga Orchestrator', {
        eventId: event.eventId,
        eventType: event.eventType,
        paymentId: payload.paymentId,
        reason: payload.reason,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
      });

      await this.sagaService.processBalanceReservationFailed(event);
      return;
    }

    if (eventType === BALANCE_REVERSED) {
      const event = envelope as BalanceReversedEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.paymentId !== 'string' ||
        !payload.paymentId.trim()
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing BalanceReversed event payload properties'
        );
      }

      this.logger.info('Processing BalanceReversed event inside Saga Orchestrator (compensation path)', {
        eventId: event.eventId,
        eventType: event.eventType,
        paymentId: payload.paymentId,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
      });

      await this.sagaService.processBalanceReversedForCompensation(event);
      return;
    }

    this.logger.info(`Ignoring non-saga balance event type: ${eventType}`, {
      eventId,
      eventType,
    });
  }
}
