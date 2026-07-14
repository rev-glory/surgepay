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
  REVERSE_LEDGER_ENTRY,
  type ReverseLedgerEntryCommand,
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

    if (eventType === RECORD_LEDGER_ENTRY) {
      await this.handleRecordLedgerEntry(envelope as RecordLedgerEntryCommand, { eventId, correlationId, sagaId });
      return;
    }

    if (eventType === REVERSE_LEDGER_ENTRY) {
      await this.handleReverseLedgerEntry(envelope as ReverseLedgerEntryCommand, { eventId, correlationId, sagaId });
      return;
    }

    this.logger.info('Unsupported command type on ledger.commands — skipping without retry', {
      eventId,
      eventType,
      correlationId,
      sagaId,
    });
  }

  private async handleRecordLedgerEntry(
    command: RecordLedgerEntryCommand,
    logCtx: { eventId: string; correlationId: string; sagaId: string }
  ): Promise<void> {
    const payload = command.payload;

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

    this.logger.info('Processing RecordLedgerEntry command', {
      ...logCtx,
      commandType: RECORD_LEDGER_ENTRY,
      paymentId: payload.paymentId,
      merchantId: payload.merchantId,
    });

    const result = await this.ledgerService.recordEntry(payload, command);

    if (result.success) {
      this.logger.info('RecordLedgerEntry processed successfully', {
        ...logCtx,
        entryId: result.entry?.id,
      });
    } else {
      this.logger.error('RecordLedgerEntry failed permanently', {
        ...logCtx,
        reason: result.reason,
      });
      // Do not throw — let the Inbox transition to PROCESSED and commit the offset.
      // LedgerRecordingFailed has been written to the outbox inside the service.
    }
  }

  private async handleReverseLedgerEntry(
    command: ReverseLedgerEntryCommand,
    logCtx: { eventId: string; correlationId: string; sagaId: string }
  ): Promise<void> {
    const payload = command.payload;

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
      typeof payload.reason !== 'string' ||
      !payload.reason.trim()
    ) {
      throw new MalformedEventEnvelopeException(
        'ReverseLedgerEntry command is missing required payload fields'
      );
    }

    this.logger.info('Processing ReverseLedgerEntry command', {
      ...logCtx,
      commandType: REVERSE_LEDGER_ENTRY,
      paymentId: payload.paymentId,
      merchantId: payload.merchantId,
    });

    const result = await this.ledgerService.reverseEntry(payload, command);

    if (result.success) {
      this.logger.info('ReverseLedgerEntry processed successfully', {
        ...logCtx,
        reversalEntryId: result.reversalEntry?.id,
      });
    } else {
      this.logger.error('ReverseLedgerEntry failed permanently', {
        ...logCtx,
        reason: result.reason,
      });
      // Permanent failure — no original entry exists. Do not throw; let Inbox commit PROCESSED.
      // The saga orchestrator will not receive LedgerReversed, and will be recovered by
      // the crash-recovery scanner in Commit 12.
    }
  }
}
