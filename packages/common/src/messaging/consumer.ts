import type { OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { type Consumer, Kafka } from 'kafkajs';

import type { ConfigService } from '@surgepay/config';
import type { BaseEventEnvelope } from '@surgepay/events';

import type { LoggerService } from '../logger';
import type { BaseInboxRepository } from './inbox.repository';
import { EventSerializer } from './serializer';

export abstract class BaseKafkaConsumer implements OnModuleInit, OnModuleDestroy {
  protected readonly kafka: Kafka;
  protected consumer!: Consumer;
  protected abstract readonly topic: string;
  protected abstract readonly groupId: string;
  protected abstract readonly inboxRepository: BaseInboxRepository;

  constructor(
    protected readonly config: ConfigService,
    protected readonly logger: LoggerService,
  ) {
    const kafkaConfig = this.config.kafka;
    this.kafka = new Kafka({
      clientId: `${kafkaConfig.clientId}-${this.constructor.name.toLowerCase()}`,
      brokers: kafkaConfig.brokers,
      ssl: kafkaConfig.ssl,
      sasl: kafkaConfig.sasl
        ? {
            mechanism: 'plain',
            username: process.env.KAFKA_SASL_USERNAME || '',
            password: process.env.KAFKA_SASL_PASSWORD || '',
          }
        : undefined,
    });
  }

  async onModuleInit(): Promise<void> {
    this.logger.setContext(this.constructor.name);
    this.logger.info(`Initializing consumer for topic ${this.topic}...`);

    this.consumer = this.kafka.consumer({
      groupId: this.groupId,
      allowAutoTopicCreation: false,
    });

    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: true });

    await this.consumer.run({
      autoCommit: false, // Explicitly disable autoCommit
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) {
          this.logger.warn('Received empty message payload (tombstone), skipping', {
            topic,
            partition,
            offset: message.offset,
          });
          return;
        }

        let envelope: BaseEventEnvelope<unknown>;
        try {
          // 1. Deserialization & Validation
          envelope = EventSerializer.deserialize(message.value);
        } catch (err) {
          this.logger.error('Failed to deserialize/validate event envelope', err, {
            topic,
            partition,
            offset: message.offset,
          });
          // Throwing ensures KafkaJS handles the error and we do not proceed.
          throw err;
        }

        // Setup logger context with Correlation ID and Event ID
        const logContext = {
          eventId: envelope.eventId,
          eventType: envelope.eventType,
          correlationId: envelope.correlationId,
          causationId: envelope.causationId,
          sagaId: envelope.sagaId,
          topic,
          partition,
          offset: message.offset,
        };

        this.logger.info(`Received event ${envelope.eventType}`, logContext);

        try {
          // 2. Persist event to local Inbox in RECEIVED state
          await this.inboxRepository.recordReceived(envelope, this.groupId);
          this.logger.info(`Durable Inbox persistence succeeded for event ${envelope.eventId}`, logContext);

          // NOTE: For Commit 5, offset commits are intentionally deferred to Commit 6
          // so we do not call commitOffsets() here.
        } catch (err) {
          this.logger.error(`Database write failed for event ${envelope.eventId}`, err, logContext);
          throw err;
        }
      },
    });

    this.logger.info(`Consumer started and running on topic ${this.topic}.`);
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info(`Disconnecting consumer from topic ${this.topic}...`);
    if (this.consumer) {
      await this.consumer.disconnect();
    }
    this.logger.info('Consumer disconnected.');
  }

  /**
   * Helper function to manually commit partition offsets
   * (Available for invocation starting in Commit 6)
   */
  async commitOffset(topic: string, partition: number, offset: string): Promise<void> {
    await this.consumer.commitOffsets([
      {
        topic,
        partition,
        offset,
      },
    ]);
  }
}
