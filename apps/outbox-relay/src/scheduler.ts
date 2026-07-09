import { Injectable, OnModuleDestroy,OnModuleInit } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { RelayService } from './relay.service';

@Injectable()
export class PollingScheduler implements OnModuleInit, OnModuleDestroy {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;

  constructor(
    private readonly configService: ConfigService,
    private readonly logger: LoggerService,
    private readonly relayService: RelayService,
  ) {
    this.logger.setContext('PollingScheduler');
  }

  onModuleInit(): void {
    this.start();
  }

  onModuleDestroy(): void {
    this.stop();
  }

  /**
   * Starts the polling scheduler execution loop.
   */
  start(): void {
    if (this.isRunning) {
      return;
    }
    this.isRunning = true;
    this.logger.info('Outbox polling scheduler started');
    void this.tick();
  }

  /**
   * Stops the polling scheduler execution loop.
   */
  stop(): void {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.logger.info('Outbox polling scheduler stopped');
  }

  /**
   * Executes a single tick and schedules the next run dynamically.
   * Chaining ticks recursively prevents concurrent executions from overlapping
   * if a single run takes longer than the configured interval.
   */
  private async tick(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    try {
      await this.relayService.runOnce();
    } catch (err) {
      this.logger.error('Unexpected error in polling cycle scheduler tick', err);
    }

    if (this.isRunning) {
      const interval = this.configService.outbox.pollingInterval;
      this.timer = setTimeout(() => {
        void this.tick();
      }, interval);
    }
  }
}
