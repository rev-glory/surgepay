import { Consumer, Kafka, EachMessagePayload } from 'kafkajs';
import { EventSerializer, EventEnvelope, InboxEvent } from '@surgepay/events';
import { LoggerService } from '@surgepay/common';
import { DuplicateEventException, EventCurrentlyProcessingException } from './duplicate-event.exception';
import { KafkaEventHandler } from './event-handler.interface';

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
}

export interface ConsumerOptions {
  groupId: string;
  topics: string[];
  fromBeginning?: boolean;
}

export abstract class BaseKafkaConsumer {
  private readonly consumerClient: Consumer;
  private readonly serializer = new EventSerializer();

  constructor(
    protected readonly kafka: Kafka,
    protected readonly persister: InboxPersister,
    protected readonly handler: KafkaEventHandler,
    protected readonly logger: LoggerService,
    protected readonly options: ConsumerOptions,
  ) {
    this.consumerClient = this.kafka.consumer({ groupId: this.options.groupId });
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

    let dbRecord: InboxEvent;
    try {
      // 1. Try to optimistic insert
      dbRecord = await this.persister.persistReceived(envelope);
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

        if (existing.status === 'PROCESSED') {
          // Skip execution (duplicate detected)
          this.logger.info('Duplicate event detected. Skipping business logic.', {
            eventId: envelope.eventId,
            consumer: this.options.groupId,
            eventType: envelope.eventType,
            correlationId: envelope.correlationId,
            sagaId: envelope.sagaId,
            duplicate: true,
          });
          return; // returns success and commits offset
        }

        if (existing.status === 'PROCESSING') {
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
    await this.processEvent(dbRecord, envelope);
  }

  /**
   * Executes transition to PROCESSING, runs handler, and transitions to PROCESSED on success.
   */
  private async processEvent(record: InboxEvent, envelope: EventEnvelope): Promise<void> {
    await this.persister.markProcessing(record.id);
    try {
      await this.handler.handle(envelope);
      await this.persister.markProcessed(record.id);
    } catch (err) {
      this.logger.error('Handler execution failed, updating inbox status', err, {
        eventId: envelope.eventId,
        consumer: this.options.groupId,
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        sagaId: envelope.sagaId,
      });
      const reason = err instanceof Error ? err.message : String(err);
      await this.persister.markFailed(record.id, reason);
      await this.persister.markRetrying(record.id, reason);
      throw err; // throw to trigger consumer-level retries
    }
  }
}
