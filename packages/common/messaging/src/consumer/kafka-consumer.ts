import { Consumer, Kafka, EachMessagePayload } from 'kafkajs';
import { EventSerializer, EventEnvelope, InboxEvent, InboxStatus, DeadLetterEvent, DeadLetterPayload } from '@surgepay/events';
import { LoggerService, MetricsService } from '@surgepay/common';
import { randomUUID } from 'crypto';
import { DuplicateEventException, EventCurrentlyProcessingException } from './duplicate-event.exception';
import { KafkaEventHandler } from './event-handler.interface';
import { DlqPublisher } from './dlq.publisher';
import { RetryPolicy, MaxAttemptsRetryPolicy } from './retry-policy';

export interface InboxPersister {
  /**
   * Finds an existing inbox event record matching the consumer group and event ID.
   */
  find(consumer: string, eventId: string): Promise<InboxEvent | null>;

  /**
   * Persists a newly received event into the Inbox database schema.
   * Starts in RECEIVED status. Throws DuplicateEventException on unique constraint violations.
   */
  persistReceived(envelope: EventEnvelope): Promise<InboxEvent>;

  /**
   * Transitions event state to PROCESSING.
   */
  markProcessing(id: string): Promise<void>;

  /**
   * Transitions event state to PROCESSED and records completion timestamp.
   */
  markProcessed(id: string): Promise<void>;

  /**
   * Transitions event state to FAILED and stores error reason.
   */
  markFailed(id: string, reason: string): Promise<void>;

  /**
   * Transitions event state to RETRYING and increments retryCount.
   */
  markRetrying(id: string, reason: string): Promise<void>;

  /**
   * Transitions event state to DLQ_SENT. Keep metadata lightweight (details in DLQ itself).
   */
  markDlqSent(id: string): Promise<void>;
}

export interface ConsumerOptions {
  groupId: string;
  topics: string[];
  fromBeginning?: boolean;
  dlqTopic: string;
  maxRetries: number;
}

export abstract class BaseKafkaConsumer {
  private readonly consumerClient: Consumer;
  private readonly serializer = new EventSerializer();
  private readonly retryPolicy: RetryPolicy;

  constructor(
    protected readonly kafka: Kafka,
    protected readonly persister: InboxPersister,
    protected readonly handler: KafkaEventHandler,
    protected readonly dlqPublisher: DlqPublisher,
    protected readonly logger: LoggerService,
    protected readonly metrics: MetricsService,
    protected readonly options: ConsumerOptions,
  ) {
    this.consumerClient = this.kafka.consumer({ groupId: this.options.groupId });
    this.retryPolicy = new MaxAttemptsRetryPolicy(this.options.maxRetries);
  }

  /**
   * Initializes connection and subscribes to configured topics.
   * Starts the Kafka message consumption loop.
   */
  async connect(): Promise<void> {
    await this.consumerClient.connect();
    this.logger.info('Kafka consumer connected successfully', { groupId: this.options.groupId });

    for (const topic of this.options.topics) {
      await this.consumerClient.subscribe({ topic, fromBeginning: this.options.fromBeginning ?? true });
      this.logger.info('Subscribed to Kafka topic', { topic, groupId: this.options.groupId });
    }

    await this.consumerClient.run({
      eachMessage: async (payload: EachMessagePayload) => {
        await this.handleMessage(payload);
      },
    });
  }

  /**
   * Disconnects consumer cleanly.
   */
  async disconnect(): Promise<void> {
    await this.consumerClient.disconnect();
    this.logger.info('Kafka consumer disconnected cleanly', { groupId: this.options.groupId });
  }

  /**
   * Processes an incoming Kafka message: deserializes, validates, checks duplicate,
   * performs state verification, and executes the registered handler.
   */
  private getServiceName(): string {
    return process.env.APP_NAME || this.options.groupId;
  }

  /**
   * Processes an incoming Kafka message: deserializes, validates, checks duplicate,
   * performs state verification, and executes the registered handler.
   */
  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    const { message, topic, partition } = payload;
    const value = message.value;

    if (!value) {
      this.logger.warn('Received empty message payload, skipping message', {
        topic,
        partition,
        offset: message.offset,
      });
      return;
    }

    let envelope: EventEnvelope;
    try {
      envelope = this.serializer.deserialize(value);
      this.serializer.validate(envelope);
    } catch (err) {
      this.logger.error('Failed to deserialize/validate message envelope', err, {
        topic,
        partition,
        offset: message.offset,
      });
      throw err;
    }

    const startTime = Date.now();
    this.metrics.incrementConsumed(this.getServiceName(), envelope.eventType, this.options.groupId);

    let dbRecord: InboxEvent;
    try {
      // 1. Try to optimistic insert
      dbRecord = await this.persister.persistReceived(envelope);
      this.metrics.incrementReceived(this.getServiceName(), envelope.eventType, this.options.groupId);
    } catch (err) {
      if (err instanceof DuplicateEventException) {
        // 2. Query existing record to reconcile status
        const existing = await this.persister.find(this.options.groupId, envelope.eventId);
        if (!existing) {
          this.logger.error('Optimistic insert unique constraint triggered but record not found during lookup', err, {
            eventId: envelope.eventId,
            consumer: this.options.groupId,
          });
          throw err;
        }

        if (existing.status === InboxStatus.PROCESSED || existing.status === InboxStatus.DLQ_SENT) {
          // Skip execution (duplicate detected or already quarantined)
          this.logger.info('Duplicate event detected (already processed/DLQed). Skipping business logic.', {
            eventId: envelope.eventId,
            consumer: this.options.groupId,
            eventType: envelope.eventType,
            correlationId: envelope.correlationId,
            sagaId: envelope.sagaId,
            duplicate: true,
          });
          this.metrics.incrementDuplicates(this.getServiceName(), envelope.eventType, this.options.groupId);
          return; // returns success and commits offset
        }

        if (existing.status === InboxStatus.PROCESSING) {
          // Concurrent or in-flight block: throw to trigger backoff retry
          throw new EventCurrentlyProcessingException(envelope.eventId, this.options.groupId);
        }

        // Otherwise (RECEIVED, FAILED, RETRYING), pick it up for processing/retry
        dbRecord = existing;
      } else {
        throw err;
      }
    }

    // 3. Execute the handler within states
    await this.processEvent(dbRecord, envelope, startTime);
  }

  /**
   * Executes transition to PROCESSING, runs handler, and transitions to PROCESSED on success.
   * If failure, evaluates retry limits via RetryPolicy to retry or route to DLQ.
   */
  private async processEvent(record: InboxEvent, envelope: EventEnvelope, startTime: number): Promise<void> {
    await this.persister.markProcessing(record.id);
    try {
      await this.handler.handle(envelope);
      await this.persister.markProcessed(record.id);
      this.metrics.incrementProcessed(this.getServiceName(), envelope.eventType, this.options.groupId);
      this.metrics.recordConsumerDuration(this.getServiceName(), envelope.eventType, this.options.groupId, Date.now() - startTime);
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      this.metrics.incrementConsumerFailures(this.getServiceName(), envelope.eventType, this.options.groupId);
      await this.persister.markFailed(record.id, reason);

      if (this.retryPolicy.shouldMoveToDlq(record.retryCount)) {
        // Retry limit exhausted -> Publish to DLQ
        this.logger.warn('Retry limit exhausted. Route event to DLQ.', {
          eventId: envelope.eventId,
          consumer: this.options.groupId,
          retryCount: record.retryCount,
          failureReason: reason,
          dlqTopic: this.options.dlqTopic,
          correlationId: envelope.correlationId,
          sagaId: envelope.sagaId,
        });

        try {
          const dlqPayload: DeadLetterPayload = {
            originalEvent: envelope,
            consumer: this.options.groupId,
            retryCount: record.retryCount,
            failureReason: reason,
            failedAt: new Date().toISOString(),
          };

          const dlqEvent = new DeadLetterEvent(dlqPayload);
          const dlqEnvelope: EventEnvelope = {
            eventId: randomUUID(),
            eventType: dlqEvent.eventType,
            version: dlqEvent.version,
            timestamp: new Date().toISOString(),
            requestId: envelope.requestId,
            correlationId: envelope.correlationId,
            causationId: envelope.causationId,
            sagaId: envelope.sagaId,
            producer: this.options.groupId,
            payload: dlqEvent.payload,
          };

          await this.dlqPublisher.publish(this.options.dlqTopic, dlqEnvelope);
          await this.persister.markDlqSent(record.id);
          this.metrics.incrementDlqEvents(this.getServiceName(), envelope.eventType, this.options.groupId);
          // Return cleanly to let KafkaJS commit the offset and progress the queue
          return;
        } catch (dlqErr) {
          this.logger.error('Fatal error during Dead Letter Queue publishing', dlqErr, {
            eventId: envelope.eventId,
          });
          throw dlqErr; // crash and let consumer restart
        }
      } else {
        // Under retry limit -> Increment retryCount and re-throw
        this.logger.info('Handler execution failed. Registering retry attempt.', {
          eventId: envelope.eventId,
          consumer: this.options.groupId,
          retryCount: record.retryCount,
          failureReason: reason,
          correlationId: envelope.correlationId,
        });
        await this.persister.markRetrying(record.id, reason);
        throw err;
      }
    }
  }
}
