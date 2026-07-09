import { Injectable } from '@nestjs/common';
import { EventEnvelope } from '@surgepay/events';
import { InboxRepository } from './inbox.repository';

@Injectable()
export class InboxService {
  constructor(private readonly repository: InboxRepository) {}

  /**
   * Manually persists an incoming event envelope to the database.
   */
  async handleIncomingEvent(envelope: EventEnvelope): Promise<void> {
    await this.repository.persistReceived(envelope);
  }

  /**
   * Transitions event state to PROCESSING.
   */
  async markProcessing(id: string): Promise<void> {
    await this.repository.markProcessing(id);
  }

  /**
   * Transitions event state to PROCESSED.
   */
  async markProcessed(id: string): Promise<void> {
    await this.repository.markProcessed(id);
  }

  /**
   * Transitions event state to FAILED.
   */
  async markFailed(id: string, reason: string): Promise<void> {
    await this.repository.markFailed(id, reason);
  }

  /**
   * Transitions event state to RETRYING.
   */
  async markRetrying(id: string, reason: string): Promise<void> {
    await this.repository.markRetrying(id, reason);
  }
}
