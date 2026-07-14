import { Injectable, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { KafkaEventProducer, LoggerService, TOPIC_REGISTRY } from '@surgepay/common';
import { OrderOutboxRepository } from '../repositories/order-outbox.repository';

@Injectable()
export class OrderOutboxRelay implements OnApplicationBootstrap, OnApplicationShutdown {
  private isRunning = false;
  private isProcessing = false;
  private timeoutId?: NodeJS.Timeout;
  private readonly pollIntervalMs = 1000; // Poll every 1 second

  constructor(
    private readonly outboxRepository: OrderOutboxRepository,
    private readonly eventProducer: KafkaEventProducer,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('OrderOutboxRelay');
  }

  async onApplicationBootstrap(): Promise<void> {
    this.logger.info('Order Outbox Relay starting...');
    this.isRunning = true;
    this.scheduleNextPoll();
  }

  async onApplicationShutdown(): Promise<void> {
    this.logger.info('Order Outbox Relay shutting down...');
    this.isRunning = false;
    if (this.timeoutId) {
      clearTimeout(this.timeoutId);
    }
  }

  private scheduleNextPoll(): void {
    if (!this.isRunning) return;

    this.timeoutId = setTimeout(async () => {
      if (!this.isRunning) return;

      if (this.isProcessing) {
        this.scheduleNextPoll();
        return;
      }

      this.isProcessing = true;
      try {
        await this.processPendingEvents();
      } catch (err) {
        this.logger.error('Error during outbox relay processing cycle', err as Error);
      } finally {
        this.isProcessing = false;
        this.scheduleNextPoll();
      }
    }, this.pollIntervalMs);
  }

  private async processPendingEvents(): Promise<void> {
    const pending = await this.outboxRepository.findPending(50);
    if (pending.length === 0) return;

    this.logger.info(`Found ${pending.length} pending outbox events to publish`);

    for (const record of pending) {
      const topic = TOPIC_REGISTRY[record.eventType];
      if (!topic) {
        this.logger.error(`No Kafka topic mapping found for event type: ${record.eventType}. Marking as FAILED.`, {
          outboxId: record.id,
        });
        await this.outboxRepository.markFailed(record.id);
        continue;
      }

      const envelope = record.payload;
      const partitionKey = envelope.sagaId || envelope.correlationId || '';

      try {
        this.logger.debug(`Publishing event ${record.eventType} to topic ${topic}`, {
          outboxId: record.id,
          eventId: envelope.eventId,
        });

        // Publish to Kafka using Saga/Correlation ID as partition key to maintain order
        await this.eventProducer.publish(topic, partitionKey, envelope as any);

        // Mark as published on success
        await this.outboxRepository.markPublished(record.id);

        this.logger.info(`Successfully relayed outbox event ${record.eventType}`, {
          outboxId: record.id,
          eventId: envelope.eventId,
        });
      } catch (publishErr) {
        this.logger.error(`Failed to publish outbox event ${record.id} to topic ${topic}. Will retry.`, publishErr as Error);
        // Leave in PENDING/RETRYING status to retry on next cycle
      }
    }
  }
}
