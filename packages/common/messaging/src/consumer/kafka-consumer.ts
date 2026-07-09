import { Consumer, Kafka, EachMessagePayload } from 'kafkajs';
import { EventSerializer, EventEnvelope } from '@surgepay/events';
import { LoggerService } from '@surgepay/common';

export interface InboxPersister {
  /**
   * Persists the received event envelope to the Inbox table.
   * Resolves immediately. If a duplicate insert fails, it should throw.
   */
  persist(envelope: EventEnvelope): Promise<void>;
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
   * Processes an incoming Kafka message by deserializing, validating, and persisting it to the Inbox.
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

    try {
      // 1. Deserialize event envelope
      const envelope = this.serializer.deserialize(value);

      // 2. Validate envelope integrity
      this.serializer.validate(envelope);

      this.logger.info('Received event, persisting to Inbox', {
        eventId: envelope.eventId,
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        topic,
        partition,
        offset: message.offset,
      });

      // 3. Persist Event Envelope to Inbox using service repository
      await this.persister.persist(envelope);

      // 4. Trigger lifecycle hook
      await this.onEventPersisted(envelope);

    } catch (err) {
      this.logger.error('Error occurred in consumer inbox persistence loop', err, {
        topic,
        partition,
        offset: message.offset,
      });
      throw err;
    }
  }

  /**
   * Hook that subclasses can override to track processing steps.
   */
  protected abstract onEventPersisted(envelope: EventEnvelope): Promise<void>;
}
