import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';

import { OutboxEvent } from './generated/client';
import { OutboxRepository } from './repositories/outbox.repository';

@Injectable()
export class OutboxPoller {
  constructor(
    private readonly outboxRepository: OutboxRepository,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('OutboxPoller');
  }

  /**
   * Polls database for pending or retrying outbox events.
   * Delegates the claim transaction to OutboxRepository.
   */
  async pollPending(batchSize: number): Promise<OutboxEvent[]> {
    return this.outboxRepository.claimPending(batchSize);
  }

  /**
   * Discovers and recovers stale events stuck in PUBLISHING state beyond the timeout,
   * transitioning them back to RETRYING or permanently FAILED.
   * Invokes the atomic recovery transaction on OutboxRepository.
   */
  async recoverStale(staleTimeoutMs: number, retryLimit: number): Promise<void> {
    await this.outboxRepository.recoverStale(staleTimeoutMs, retryLimit);
  }
}
