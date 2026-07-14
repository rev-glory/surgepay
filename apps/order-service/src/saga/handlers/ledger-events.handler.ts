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
  LEDGER_ENTRY_RECORDED,
  LEDGER_RECORDING_FAILED,
  LEDGER_REVERSED,
  type LedgerEntryRecordedEvent,
  type LedgerRecordingFailedEvent,
  type LedgerReversedEvent,
} from '@surgepay/events';

import { OrderInboxRepository } from '../../repositories/inbox.repository';
import { SagaService } from '../saga.service';

@Injectable()
export class SagaLedgerEventsConsumer extends BaseKafkaConsumer {
  protected readonly topic = 'ledger.events';
  protected readonly groupId = `${this.config.kafka.consumerGroupId}-saga-ledger`;

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

    if (eventType === LEDGER_ENTRY_RECORDED) {
      const event = envelope as LedgerEntryRecordedEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.paymentId !== 'string' ||
        !payload.paymentId.trim()
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing LedgerEntryRecorded event payload properties'
        );
      }

      this.logger.info('Processing LedgerEntryRecorded event inside Saga Orchestrator', {
        eventId: event.eventId,
        eventType: event.eventType,
        paymentId: payload.paymentId,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
      });

      await this.sagaService.processLedgerEntryRecorded(event);
      return;
    }

    if (eventType === LEDGER_RECORDING_FAILED) {
      const event = envelope as LedgerRecordingFailedEvent;
      const payload = event.payload;

      if (!payload || typeof payload !== 'object' || !payload.reason) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing LedgerRecordingFailed event payload properties'
        );
      }

      this.logger.info('Processing LedgerRecordingFailed event inside Saga Orchestrator', {
        eventId: event.eventId,
        eventType: event.eventType,
        paymentId: payload.paymentId,
        reason: payload.reason,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
      });

      await this.sagaService.processLedgerRecordingFailed(event);
      return;
    }

    if (eventType === LEDGER_REVERSED) {
      const event = envelope as LedgerReversedEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.paymentId !== 'string' ||
        !payload.paymentId.trim() ||
        typeof payload.originalEntryId !== 'string' ||
        !payload.originalEntryId.trim()
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing LedgerReversed event payload properties'
        );
      }

      this.logger.info('Processing LedgerReversed event inside Saga Orchestrator (compensation path)', {
        eventId: event.eventId,
        eventType: event.eventType,
        paymentId: payload.paymentId,
        originalEntryId: payload.originalEntryId,
        correlationId: event.correlationId,
        sagaId: event.sagaId,
      });

      await this.sagaService.processLedgerReversed(event);
      return;
    }

    this.logger.info(`Ignoring non-saga ledger event type: ${eventType}`, {
      eventId,
      eventType,
    });
  }
}
