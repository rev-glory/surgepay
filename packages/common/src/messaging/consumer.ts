import type { OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { type Consumer, Kafka } from 'kafkajs';

import type { ConfigService } from '@surgepay/config';
import type { BaseEventEnvelope } from '@surgepay/events';

import type { LoggerService } from '../logger';
import type { BaseInboxRepository } from './inbox.repository';
import { EventSerializer } from './serializer';

export class EventProcessingInProgressException extends Error {
  constructor(eventId: string, consumer: string) {
    super(`Event ${eventId} is currently being processed by consumer ${consumer}.`);
    this.name = 'EventProcessingInProgressException';
  }
}

function isPrismaUniqueConstraintError(err: unknown): boolean {
  return (
    err !== null &&
    typeof err === 'object' &&
    'code' in err &&
    (err as { code: string }).code === 'P2002'
  );
}

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

  protected abstract handleEvent(envelope: BaseEventEnvelope<unknown>): Promise<void>;

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
          this.logger.error('Failed to deserialize/validate event envelope', err as Error, {
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

        // 2. Check duplicate state
        const existing = await this.inboxRepository.findByEventIdAndConsumer(envelope.eventId, this.groupId);

        if (existing) {
          if (existing.status === 'PROCESSED') {
            this.logger.info('Duplicate event delivery skipped (already processed)', {
              ...logContext,
              duplicate: true,
              inboxStatus: existing.status,
            });
            await this.commitOffset(topic, partition, (BigInt(message.offset) + 1n).toString());
            return;
          }

          if (existing.status === 'PROCESSING') {
            this.logger.warn('Duplicate event delivery skipped - processing in progress', {
              ...logContext,
              duplicate: true,
              inboxStatus: existing.status,
            });
            throw new EventProcessingInProgressException(envelope.eventId, this.groupId);
          }

          // If status is RECEIVED or RETRYING, we proceed to transition and process it
        } else {
          // First time delivery, record as RECEIVED
          try {
            await this.inboxRepository.recordReceived(envelope, this.groupId);
            this.logger.info(`Durable Inbox persistence succeeded for event ${envelope.eventId}`, logContext);
          } catch (err) {
            if (isPrismaUniqueConstraintError(err)) {
              this.logger.info('Duplicate event delivery detected via DB constraint (concurrent insert)', {
                ...logContext,
                duplicate: true,
              });

              // Re-evaluate the status of the record in DB
              const collided = await this.inboxRepository.findByEventIdAndConsumer(envelope.eventId, this.groupId);
              if (collided) {
                if (collided.status === 'PROCESSED') {
                  await this.commitOffset(topic, partition, (BigInt(message.offset) + 1n).toString());
                  return;
                }
                if (collided.status === 'PROCESSING') {
                  throw new EventProcessingInProgressException(envelope.eventId, this.groupId);
                }
              }
            } else {
              this.logger.error(`Database write failed for event ${envelope.eventId}`, err as Error, logContext);
              throw err;
            }
          }
        }

        // 3. Atomically transition from (RECEIVED, RETRYING) to PROCESSING to claim lock
        const transitioned = await this.inboxRepository.transitionStatus(
          envelope.eventId,
          this.groupId,
          ['RECEIVED', 'RETRYING'],
          'PROCESSING',
        );

        if (!transitioned) {
          this.logger.info('Duplicate event delivery skipped (concurrency lock acquired by another worker)', {
            ...logContext,
            duplicate: true,
          });
          // Query DB again to see if it is PROCESSING or PROCESSED
          const current = await this.inboxRepository.findByEventIdAndConsumer(envelope.eventId, this.groupId);
          if (current && current.status === 'PROCESSED') {
            await this.commitOffset(topic, partition, (BigInt(message.offset) + 1n).toString());
            return;
          }
          throw new EventProcessingInProgressException(envelope.eventId, this.groupId);
        }

        this.logger.info(`Acquired processing lock for event ${envelope.eventId}, executing handler`, logContext);

        // 4. Execute the business handler
        try {
          await this.handleEvent(envelope);

          // Mark as PROCESSED
          await this.inboxRepository.updateStatus(envelope.eventId, this.groupId, 'PROCESSED');

          // Commit partition offset
          await this.commitOffset(topic, partition, (BigInt(message.offset) + 1n).toString());
          this.logger.info(`Successfully processed event ${envelope.eventId} and committed offset`, logContext);
        } catch (handlerErr) {
          this.logger.error(`Handler execution failed for event ${envelope.eventId}`, handlerErr as Error, logContext);

          // Transition back to RETRYING to allow future processing runs
          const currentRecord = await this.inboxRepository.findByEventIdAndConsumer(envelope.eventId, this.groupId);
          const currentRetry = currentRecord?.retryCount ?? 0;

          await this.inboxRepository.updateStatus(
            envelope.eventId,
            this.groupId,
            'RETRYING',
            currentRetry + 1,
          );

          throw handlerErr;
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
