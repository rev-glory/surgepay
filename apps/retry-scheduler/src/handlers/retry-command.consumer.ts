import { Injectable, Optional } from '@nestjs/common';
import {
  BaseKafkaConsumer,
  KafkaEventProducer,
  LoggerService,
  MalformedEventEnvelopeException,
  MetricsService,
} from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import { BaseEventEnvelope, SCHEDULE_RETRY, type ScheduleRetryCommand } from '@surgepay/events';
import { RetryInboxRepository } from '../repositories/inbox.repository';
import { RetrySchedulerService } from '../services/retry-scheduler.service';

@Injectable()
export class RetryCommandConsumer extends BaseKafkaConsumer {
  protected readonly topic = 'retry.commands';
  protected readonly groupId = `${this.config.kafka.consumerGroupId}-retry-scheduler-commands`;

  constructor(
    config: ConfigService,
    logger: LoggerService,
    protected readonly inboxRepository: RetryInboxRepository,
    eventProducer: KafkaEventProducer,
    private readonly retryService: RetrySchedulerService,
    @Optional() protected readonly metrics?: MetricsService
  ) {
    super(config, logger, eventProducer, metrics);
  }

  protected async handleEvent(envelope: BaseEventEnvelope<unknown>): Promise<void> {
    const { eventType, eventId } = envelope;

    if (eventType !== SCHEDULE_RETRY) {
      this.logger.info(`Ignoring non-retry command type: ${eventType}`, {
        eventId,
        eventType,
      });
      return;
    }

    const command = envelope as ScheduleRetryCommand;
    const payload = command.payload;

    if (
      !payload ||
      typeof payload !== 'object' ||
      typeof payload.originalTopic !== 'string' ||
      !payload.originalEvent ||
      typeof payload.retryCount !== 'number' ||
      typeof payload.maxAttempts !== 'number' ||
      typeof payload.baseDelayMs !== 'number' ||
      typeof payload.maxDelayMs !== 'number'
    ) {
      throw new MalformedEventEnvelopeException(
        'Invalid or missing ScheduleRetry command payload properties'
      );
    }

    await this.retryService.schedule(payload);
  }
}
