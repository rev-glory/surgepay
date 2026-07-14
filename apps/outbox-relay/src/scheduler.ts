import { Injectable, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { OutboxRelayService } from './relay.service';

@Injectable()
export class OutboxScheduler implements OnApplicationBootstrap, OnApplicationShutdown {
  private isRunning = false;
  private isPolling = false;
  private timeoutId?: NodeJS.Timeout;

  constructor(
    private readonly relayService: OutboxRelayService,
    private readonly config: ConfigService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('OutboxScheduler');
  }

  async onApplicationBootstrap(): Promise<void> {
    this.logger.info('Outbox Relay Scheduler starting...', {
      pollingInterval: this.config.outbox.pollingInterval,
      batchSize: this.config.outbox.batchSize,
    });
    this.isRunning = true;
    this.scheduleNextPoll();
  }

  async onApplicationShutdown(): Promise<void> {
    this.logger.info('Outbox Relay Scheduler shutting down gracefully...');
    this.isRunning = false;
    if (this.timeoutId) {
      clearTimeout(this.timeoutId);
    }
  }

  private scheduleNextPoll(): void {
    if (!this.isRunning) {
      return;
    }

    const interval = this.config.outbox.flushInterval;
    this.timeoutId = setTimeout(async () => {
      if (!this.isRunning) return;

      // Prevent overlapping cycles
      if (this.isPolling) {
        this.logger.warn('Previous polling cycle is still active. Skipping this cycle to prevent overlap.');
        this.scheduleNextPoll();
        return;
      }

      this.isPolling = true;
      try {
        await this.relayService.processBatch();
      } catch (err) {
        // Recoverable failure: log and let scheduler continue according to lifecycle
        this.logger.error('Recoverable failure in outbox polling cycle. Continuing scheduler.', err);
      } finally {
        this.isPolling = false;
        this.scheduleNextPoll();
      }
    }, interval);
  }
}
