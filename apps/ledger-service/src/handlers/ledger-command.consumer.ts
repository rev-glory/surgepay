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
import {
  type BaseEventEnvelope,
  RECORD_LEDGER_ENTRY,
  type RecordLedgerEntryCommand,
} from '@surgepay/events';

import { LedgerInboxRepository } from '../repositories/inbox.repository';
import { LedgerService } from '../services/ledger.service';

@Injectable()
export class LedgerCommandConsumer extends BaseKafkaConsumer {
  protected readonly topic = TOPIC_REGISTRY[RECORD_LEDGER_ENTRY] ?? 'ledger.commands';
  protected readonly groupId = `${this.config.kafka.consumerGroupId}-ledger-commands`;

  constructor(
    config: ConfigService,
    logger: LoggerService,
    protected readonly inboxRepository: LedgerInboxRepository,
    eventProducer: KafkaEventProducer,
    private readonly ledgerService: LedgerService,
    @Optional() protected readonly metrics?: MetricsService
  ) {
    super(config, logger, eventProducer, metrics);
  }

  protected async handleEvent(envelope: BaseEventEnvelope<unknown>): Promise<void> {
    const { eventId, eventType, correlationId, sagaId } = envelope;

    if (eventType !== RECORD_LEDGER_ENTRY) {
      this.logger.info('Unsupported command type on ledger.commands — skipping without retry', {
        eventId,
        eventType,
        correlationId,
        sagaId,
      });
      return;
    }

    const command = envelope as RecordLedgerEntryCommand;
    const payload = command.payload;

    // Validate command envelope metadata and payload
    if (
      !payload ||
      typeof payload !== 'object' ||
      typeof payload.paymentId !== 'string' ||
      !payload.paymentId.trim() ||
      typeof payload.merchantId !== 'string' ||
      !payload.merchantId.trim() ||
      typeof payload.amount !== 'number' ||
      typeof payload.currency !== 'string' ||
      !payload.currency.trim() ||
      typeof payload.entryType !== 'string' ||
      !payload.entryType.trim()
    ) {
      throw new MalformedEventEnvelopeException(
        'RecordLedgerEntry command is missing required payload fields'
      );
    }

    const logContext = {
      commandId: eventId,
      commandType: eventType,
      paymentId: payload.paymentId,
      merchantId: payload.merchantId,
      correlationId,
      sagaId,
    };

    this.logger.info('Processing RecordLedgerEntry command', logContext);

    // Delegate to the domain logic layer
    const result = await this.ledgerService.recordEntry(payload, command);

    if (result.success) {
      this.logger.info('RecordLedgerEntry processed successfully', {
        ...logContext,
        entryId: result.entry?.id,
      });
    } else {
      this.logger.error('RecordLedgerEntry failed permanently', {
        ...logContext,
        reason: result.reason,
      });
      // Do not throw an exception here. Let the function return successfully so that
      // the Inbox record transitions to PROCESSED and the offset is committed.
      // The failure event (LedgerRecordingFailed) has been persisted to the Outbox.
    }
  }
}
