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
  SAGA_RETRY_REGISTERED,
  SAGA_STEP_EXECUTION_FAILED,
  type SagaRetryRegisteredEvent,
  type SagaStepExecutionFailedEvent,
} from '@surgepay/events';
import { OrderInboxRepository } from '../../repositories/inbox.repository';
import { SagaService } from '../saga.service';

@Injectable()
export class RetryEventsConsumer extends BaseKafkaConsumer {
  protected readonly topic = 'retry.events';
  protected readonly groupId = `${this.config.kafka.consumerGroupId}-saga-retry-events`;

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
    const { eventType, eventId, sagaId } = envelope;

    if (eventType !== SAGA_RETRY_REGISTERED && eventType !== SAGA_STEP_EXECUTION_FAILED) {
      this.logger.info(`Ignoring non-saga retry event type: ${eventType}`, {
        eventId,
        eventType,
      });
      return;
    }

    if (!sagaId) {
      throw new MalformedEventEnvelopeException('Saga ID is missing from event envelope');
    }

    if (eventType === SAGA_RETRY_REGISTERED) {
      const event = envelope as SagaRetryRegisteredEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.originalEventId !== 'string' ||
        typeof payload.attempt !== 'number' ||
        typeof payload.nextExecutionTime !== 'string'
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing SagaRetryRegistered event payload properties'
        );
      }

      await this.sagaService.processRetryRegistered(event);
    } else if (eventType === SAGA_STEP_EXECUTION_FAILED) {
      const event = envelope as SagaStepExecutionFailedEvent;
      const payload = event.payload;

      if (
        !payload ||
        typeof payload !== 'object' ||
        typeof payload.originalEventId !== 'string' ||
        typeof payload.failureReason !== 'string'
      ) {
        throw new MalformedEventEnvelopeException(
          'Invalid or missing SagaStepExecutionFailed event payload properties'
        );
      }

      await this.sagaService.processStepExecutionFailed(event);
    }
  }
}
