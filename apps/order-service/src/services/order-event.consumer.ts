import { Injectable, Optional } from '@nestjs/common';
import { randomUUID } from 'crypto';

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
  CHECK_ORDER_ELIGIBILITY,
  type CheckOrderEligibilityCommand,
  ORDER_ELIGIBILITY_CONFIRMED,
  ORDER_ELIGIBILITY_REJECTED,
  type OrderEligibilityConfirmedEvent,
  type OrderEligibilityRejectedEvent,
} from '@surgepay/events';

import { OrderInboxRepository } from '../repositories/inbox.repository';
import { type OrderEligibilityResult, OrderService } from './order.service';

@Injectable()
export class OrderEventConsumer extends BaseKafkaConsumer {
  // Subscribes to order.commands — the topic on which the Saga Orchestrator dispatches
  // eligibility check commands. topic name is roadmap-defined (commits.txt Commit 5).
  protected readonly topic = TOPIC_REGISTRY[CHECK_ORDER_ELIGIBILITY] ?? 'order.commands';

  // Distinct group ID from the Saga Orchestrator's own consumers to ensure full
  // message delivery to this consumer without partition-splitting.
  protected readonly groupId = `${this.config.kafka.consumerGroupId}-order-commands`;

  constructor(
    config: ConfigService,
    logger: LoggerService,
    protected readonly inboxRepository: OrderInboxRepository,
    eventProducer: KafkaEventProducer,
    private readonly orderService: OrderService,
    @Optional() protected readonly metrics?: MetricsService,
  ) {
    super(config, logger, eventProducer, metrics);
  }

  /**
   * Processes a single CheckOrderEligibility command after the BaseKafkaConsumer
   * framework has performed Inbox deduplication.
   *
   * Guarantees:
   * - Unsupported command types are logged and skipped; the Inbox is marked PROCESSED
   *   and the offset is committed so they are never redelivered.
   * - Business rejections (ORDER_NOT_FOUND etc.) cause exactly one OrderEligibilityRejected
   *   event to be published; no retry is triggered.
   * - Infrastructure failures (DB or broker errors) propagate as thrown exceptions.
   *   The BaseKafkaConsumer will transition the Inbox record to RETRYING and leave
   *   the offset uncommitted so Kafka redelivers the command.
   */
  protected async handleEvent(envelope: BaseEventEnvelope<unknown>): Promise<void> {
    const { eventId, eventType, correlationId, sagaId } = envelope;

    // Filter unsupported command types. Return (not throw) so the framework marks
    // this Inbox record as PROCESSED and commits the offset — preventing redelivery.
    if (eventType !== CHECK_ORDER_ELIGIBILITY) {
      this.logger.info('Unsupported command type on order.commands — skipping without retry', {
        eventId,
        eventType,
        correlationId,
        sagaId,
      });
      return;
    }

    const command = envelope as CheckOrderEligibilityCommand;
    const payload = command.payload;

    // Validate all required payload fields before delegating to the domain layer.
    // A malformed envelope is a permanent failure — throw so the consumer routes to DLQ.
    if (
      !payload ||
      typeof payload !== 'object' ||
      typeof payload.orderId !== 'string' ||
      !payload.orderId.trim() ||
      typeof payload.paymentId !== 'string' ||
      !payload.paymentId.trim() ||
      typeof payload.merchantId !== 'string' ||
      !payload.merchantId.trim() ||
      typeof payload.amount !== 'number' ||
      typeof payload.currency !== 'string' ||
      !payload.currency.trim()
    ) {
      throw new MalformedEventEnvelopeException(
        'CheckOrderEligibility command is missing required payload fields',
      );
    }

    const { orderId, paymentId, merchantId, amount, currency } = payload;

    const logContext = {
      commandId: eventId,
      commandType: eventType,
      orderId,
      paymentId,
      merchantId,
      correlationId,
      sagaId,
    };

    this.logger.info('Processing CheckOrderEligibility command', logContext);

    // Delegate to the Order aggregate domain layer. This method never throws for
    // business rejections — they surface as typed values in the result union.
    // Infrastructure errors (DB unavailable) still throw and propagate upward.
    const result: OrderEligibilityResult = await this.orderService.validateOrderEligibilityById({
      orderId,
      paymentId,
      merchantId,
      amount,
      currency,
    });

    // Build and publish the result event. Both happy and rejection paths:
    //   - Use a freshly generated eventId (never reuse the incoming commandId)
    //   - Preserve correlationId and sagaId from the command (Saga Orchestrator correlation)
    //   - Set causationId = command's eventId (establishes the causal link)
    const responseEventId = randomUUID();
    const timestamp = new Date().toISOString();

    if (result.eligible) {
      const confirmedEvent: OrderEligibilityConfirmedEvent = {
        eventId: responseEventId,
        eventType: ORDER_ELIGIBILITY_CONFIRMED,
        correlationId,
        causationId: eventId,
        sagaId,
        timestamp,
        version: 1,
        payload: {
          orderId: result.order.id,
        },
      };

      await this.eventProducer.publish(
        TOPIC_REGISTRY[ORDER_ELIGIBILITY_CONFIRMED] ?? 'order.events',
        sagaId,
        confirmedEvent,
      );

      this.logger.info('Published OrderEligibilityConfirmed', {
        ...logContext,
        responseEventId,
        topic: TOPIC_REGISTRY[ORDER_ELIGIBILITY_CONFIRMED] ?? 'order.events',
      });
    } else {
      const rejectedEvent: OrderEligibilityRejectedEvent = {
        eventId: responseEventId,
        eventType: ORDER_ELIGIBILITY_REJECTED,
        correlationId,
        causationId: eventId,
        sagaId,
        timestamp,
        version: 1,
        payload: {
          orderId: result.orderId,
          reason: result.reason,
        },
      };

      await this.eventProducer.publish(
        TOPIC_REGISTRY[ORDER_ELIGIBILITY_REJECTED] ?? 'order.events',
        sagaId,
        rejectedEvent,
      );

      this.logger.info('Published OrderEligibilityRejected', {
        ...logContext,
        responseEventId,
        reason: result.reason,
        topic: TOPIC_REGISTRY[ORDER_ELIGIBILITY_REJECTED] ?? 'order.events',
      });
    }
  }
}
