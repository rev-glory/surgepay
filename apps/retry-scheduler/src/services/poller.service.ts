import { Injectable, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { KafkaEventProducer, LoggerService } from '@surgepay/common';
import { RetryRepository } from '../repositories/retry.repository';

@Injectable()
export class RetryPollerService implements OnApplicationBootstrap, OnApplicationShutdown {
  private isRunning = false;
  private isPolling = false;
  private timeoutId?: NodeJS.Timeout;
  private readonly pollIntervalMs = 2000; // Poll every 2 seconds

  constructor(
    private readonly retryRepository: RetryRepository,
    private readonly eventProducer: KafkaEventProducer,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('RetryPollerService');
  }

  async onApplicationBootstrap(): Promise<void> {
    this.logger.info('Retry Poller starting...');
    this.isRunning = true;
    this.scheduleNextPoll();
  }

  async onApplicationShutdown(): Promise<void> {
    this.logger.info('Retry Poller shutting down...');
    this.isRunning = false;
    if (this.timeoutId) {
      clearTimeout(this.timeoutId);
    }
  }

  private scheduleNextPoll(): void {
    if (!this.isRunning) return;

    this.timeoutId = setTimeout(async () => {
      if (!this.isRunning) return;

      if (this.isPolling) {
        this.scheduleNextPoll();
        return;
      }

      this.isPolling = true;
      try {
        await this.pollAndExecute();
      } catch (err) {
        this.logger.error('Error during retry poller execution cycle', err as Error);
      } finally {
        this.isPolling = false;
        this.scheduleNextPoll();
      }
    }, this.pollIntervalMs);
  }

  private async pollAndExecute(): Promise<void> {
    const now = new Date();
    const dueRetries = await this.retryRepository.findDue(now, 50);

    if (dueRetries.length === 0) return;

    this.logger.info(`Found ${dueRetries.length} due retries to execute`);

    for (const record of dueRetries) {
      try {
        this.logger.info(`Executing retry ${record.id} for event ${record.originalMessage.eventId} to topic ${record.originalTopic}`);

        const partitionKey = record.sagaId || record.correlationId;

        // At-least-once: Republish before marking DB status to recover on crash
        await this.eventProducer.publish(record.originalTopic, partitionKey, record.originalMessage as any);

        await this.retryRepository.markExecuted(record.id);

        this.logger.info(`Successfully executed retry ${record.id}`);
      } catch (err) {
        this.logger.error(`Failed to execute retry ${record.id}`, err as Error);
      }
    }
  }
}
