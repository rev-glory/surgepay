import { Inject, Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { OutboxPoller } from './poller';
import { EVENT_PUBLISHER, EventPublisher } from './publisher';

@Injectable()
export class OutboxRelayService {
  constructor(
    private readonly poller: OutboxPoller,
    @Inject(EVENT_PUBLISHER) private readonly publisher: EventPublisher,
    private readonly config: ConfigService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('OutboxRelayService');
  }

  /**
   * Orchestrates a single polling and publishing batch cycle.
   */
  async processBatch(): Promise<void> {
    const batchSize = this.config.outbox.batchSize;
    
    this.logger.debug('Polling cycle started', { batchSize });

    // 1. Acquire pending events from poller
    const events = await this.poller.pollPending(batchSize);

    if (events.length === 0) {
      this.logger.debug('No pending outbox events found. Skipping delegation.');
      return;
    }

    this.logger.info('Pending events discovered', {
      count: events.length,
      batchSize,
    });

    // 2. Delegate each discovered event to the publisher abstraction
    for (const event of events) {
      try {
        await this.publisher.publish(event);
        
        // Note: Commit 1 intentionally leaves the database Outbox status unchanged as 'PENDING'.
        // Real Kafka publishing is in Commit 2, and publish acknowledgements are in Commit 3.
      } catch (err) {
        // If publisher fails, log and propagate the failure to the polling cycle boundary.
        this.logger.error('Publisher boundary failure. Propagating to scheduler.', err, {
          eventId: event.id,
          eventType: event.eventType,
          correlationId: event.correlationId,
        });
        throw err;
      }
    }

    this.logger.info('Polling cycle completed successfully', {
      processedCount: events.length,
    });
  }
}
