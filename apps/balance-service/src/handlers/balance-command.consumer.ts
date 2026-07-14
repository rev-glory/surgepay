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
  BaseEventEnvelope,
  RESERVE_BALANCE,
  ReserveBalanceCommand,
  REVERSE_BALANCE,
  ReverseBalanceCommand,
} from '@surgepay/events';

import { BalanceInboxRepository } from '../repositories/inbox.repository';
import { BalanceService } from '../services/balance.service';

@Injectable()
export class BalanceCommandConsumer extends BaseKafkaConsumer {
  protected readonly topic = TOPIC_REGISTRY[RESERVE_BALANCE] ?? 'balance.commands';
  protected readonly groupId = `${this.config.kafka.consumerGroupId}-balance-commands`;

  constructor(
    config: ConfigService,
    logger: LoggerService,
    protected readonly inboxRepository: BalanceInboxRepository,
    eventProducer: KafkaEventProducer,
    private readonly balanceService: BalanceService,
    @Optional() protected readonly metrics?: MetricsService
  ) {
    super(config, logger, eventProducer, metrics);
  }

  protected async handleEvent(envelope: BaseEventEnvelope<unknown>): Promise<void> {
    const { eventId, eventType, correlationId, sagaId } = envelope;

    if (eventType === RESERVE_BALANCE) {
      await this.handleReserveBalance(envelope as ReserveBalanceCommand, { eventId, correlationId, sagaId });
      return;
    }

    if (eventType === REVERSE_BALANCE) {
      await this.handleReverseBalance(envelope as ReverseBalanceCommand, { eventId, correlationId, sagaId });
      return;
    }

    this.logger.info('Unsupported command type on balance.commands — skipping without retry', {
      eventId,
      eventType,
      correlationId,
      sagaId,
    });
  }

  private async handleReserveBalance(
    command: ReserveBalanceCommand,
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
      !payload.currency.trim()
    ) {
      throw new MalformedEventEnvelopeException(
        'ReserveBalance command is missing required payload fields'
      );
    }

    this.logger.info('Processing ReserveBalance command', {
      ...logCtx,
      commandType: RESERVE_BALANCE,
      paymentId: payload.paymentId,
      merchantId: payload.merchantId,
    });

    const result = await this.balanceService.reserve(payload, command);

    if (result.success) {
      this.logger.info('ReserveBalance processed successfully', logCtx);
    } else {
      this.logger.error('ReserveBalance failed permanently', {
        ...logCtx,
        reason: result.reason,
      });
      // Do not throw — let Inbox transition to PROCESSED and commit the offset.
      // BalanceReservationFailed has been written to the outbox inside the service.
    }
  }

  private async handleReverseBalance(
    command: ReverseBalanceCommand,
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
        'ReverseBalance command is missing required payload fields'
      );
    }

    this.logger.info('Processing ReverseBalance command', {
      ...logCtx,
      commandType: REVERSE_BALANCE,
      paymentId: payload.paymentId,
      merchantId: payload.merchantId,
    });

    const result = await this.balanceService.reverse(payload, command);

    if (result.success) {
      this.logger.info('ReverseBalance processed successfully', logCtx);
    } else {
      this.logger.error('ReverseBalance failed permanently', {
        ...logCtx,
        reason: result.reason,
      });
      // Do not throw — the Inbox will commit PROCESSED and offset.
      // The saga orchestrator will not receive BalanceReversed, and will be
      // recovered by the crash-recovery scanner in Commit 12.
    }
  }
}
