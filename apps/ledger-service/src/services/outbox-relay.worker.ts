import { Injectable, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';

import { KafkaEventProducer, LoggerService, TOPIC_REGISTRY } from '@surgepay/common';

import { OutboxRepository } from '../repositories/outbox.repository';

@Injectable()
export class OutboxRelayWorker implements OnApplicationBootstrap, OnApplicationShutdown {
  private isRunning = false;
  private isPolling = false;
  private timeoutId?: NodeJS.Timeout;
  private readonly flushInterval = 500; // ms
  private readonly retryLimit = 5;

  constructor(
    private readonly outboxRepository: OutboxRepository,
    private readonly producer: KafkaEventProducer,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('OutboxRelayWorker');
  }

  async onApplicationBootstrap(): Promise<void> {
    this.logger.info('Ledger Outbox Relay Worker starting...');
    this.isRunning = true;
    this.scheduleNextPoll();
  }

  async onApplicationShutdown(): Promise<void> {
    this.logger.info('Ledger Outbox Relay Worker shutting down...');
    this.isRunning = false;
    if (this.timeoutId) {
      clearTimeout(this.timeoutId);
    }
  }

  private scheduleNextPoll(): void {
    if (!this.isRunning) {
      return;
    }

    this.timeoutId = setTimeout(async () => {
      if (!this.isRunning) return;

      if (this.isPolling) {
        this.scheduleNextPoll();
        return;
      }

      this.isPolling = true;
      try {
        await this.processBatch();
      } catch (err) {
        this.logger.error('Error in Outbox Relay processing cycle', err as Error);
      } finally {
        this.isPolling = false;
        this.scheduleNextPoll();
      }
    }, this.flushInterval);
  }

  async processBatch(): Promise<void> {
    const events = await this.outboxRepository.findPending(10);
    if (events.length === 0) {
      return;
    }

    this.logger.info(`Polling found ${events.length} pending outbox events to publish`);

    for (const event of events) {
      const topic = TOPIC_REGISTRY[event.eventType];
      if (!topic) {
        this.logger.error(`No topic mapping found for event type: ${event.eventType}. Marking as failed.`);
        await this.outboxRepository.markFailed(event.id);
        continue;
      }

      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const envelope = event.payload as any;
        // aggregateId represents the paymentId or similar partition key
        await this.producer.publish(
          topic,
          event.aggregateId,
          envelope,
          (event.traceHeaders as Record<string, string>) || undefined
        );

        await this.outboxRepository.markPublished(event.id);

        this.logger.info('Event published successfully to Kafka', {
          outboxEventId: event.id,
          eventType: event.eventType,
          topic,
        });
      } catch (publishErr: unknown) {
        this.logger.error(`Failed to publish outbox event ${event.id}`, publishErr as Error);
        if (event.retryCount >= this.retryLimit) {
          this.logger.warn(`Retry limit of ${this.retryLimit} reached for outbox event ${event.id}. Marking as failed.`);
          await this.outboxRepository.markFailed(event.id);
        } else {
          await this.outboxRepository.incrementRetry(event.id);
        }
      }
    }
  }
}
