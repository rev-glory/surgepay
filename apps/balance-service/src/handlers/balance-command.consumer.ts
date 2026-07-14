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

    if (eventType !== RESERVE_BALANCE) {
      this.logger.info('Unsupported command type on balance.commands — skipping without retry', {
        eventId,
        eventType,
        correlationId,
        sagaId,
      });
      return;
    }

    const command = envelope as ReserveBalanceCommand;
    const payload = command.payload;

    // Validate payload envelope fields
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

    const logContext = {
      commandId: eventId,
      commandType: eventType,
      paymentId: payload.paymentId,
      merchantId: payload.merchantId,
      correlationId,
      sagaId,
    };

    this.logger.info('Processing ReserveBalance command', logContext);

    // Delegate to the business logic layer
    const result = await this.balanceService.reserve(payload, command);

    if (result.success) {
      this.logger.info('ReserveBalance processed successfully', logContext);
    } else {
      this.logger.error('ReserveBalance failed permanently', {
        ...logContext,
        reason: result.reason,
      });
      // Do not throw an exception here. Let the function return successfully so that
      // the Inbox record transitions to PROCESSED and the offset is committed.
      // The failure event (BalanceReservationFailed) has been persisted to the Outbox.
    }
  }
}
