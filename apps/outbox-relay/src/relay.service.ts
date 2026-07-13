import { Inject, Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { OutboxPoller } from './poller';
import { EVENT_PUBLISHER, EventPublisher } from './publisher';
import { OutboxRepository } from './repositories/outbox.repository';

@Injectable()
export class OutboxRelayService {
  constructor(
    private readonly poller: OutboxPoller,
    private readonly outboxRepository: OutboxRepository,
    @Inject(EVENT_PUBLISHER) private readonly publisher: EventPublisher,
    private readonly config: ConfigService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('OutboxRelayService');
  }

  /**
   * Orchestrates a single polling and publishing batch cycle.
   * Coordinates stale claim recovery, claiming eligible events,
   * delegating to the publisher, and committing final states.
   */
  async processBatch(): Promise<void> {
    const staleTimeout = this.config.outbox.staleTimeoutMs;
    const retryLimit = this.config.outbox.retryLimit;
    const batchSize = this.config.outbox.batchSize;

    this.logger.debug('Polling cycle started', { batchSize });

    // 1. Recover stale claims (PUBLISHING -> FAILED -> RETRYING/FAILED)
    await this.poller.recoverStale(staleTimeout, retryLimit);

    // 2. Acquire pending events from poller (PENDING/RETRYING -> PUBLISHING)
    const events = await this.poller.pollPending(batchSize);

    if (events.length === 0) {
      this.logger.debug('No pending outbox events found. Skipping delegation.');
      return;
    }

    this.logger.info('Pending events claimed for publication', {
      count: events.length,
      batchSize,
    });

    // 3. Delegate each discovered event to the publisher abstraction
    for (const event of events) {
      try {
        const metadata = await this.publisher.publish(event);

        // 4. Atomically record PUBLISHED state and broker metadata
        await this.outboxRepository.markPublished(event.id, metadata.partition, metadata.offset);
      } catch (err) {
        this.logger.error('Publisher boundary failure. Coordinating rollback transitions.', err, {
          eventId: event.id,
          eventType: event.eventType,
          correlationId: event.correlationId,
        });

        try {
          // Transition: PUBLISHING -> FAILED (increments retry count)
          const failedEvent = await this.outboxRepository.markFailed(
            event.id,
            err instanceof Error ? err.message : String(err),
          );

          // Transition: FAILED -> RETRYING if within retry limits
          if (failedEvent.retryCount < retryLimit) {
            await this.outboxRepository.markRetrying(event.id);
          } else {
            this.logger.error('Outbox event retry limit exhausted. Left in FAILED state.', {
              eventId: event.id,
              eventType: event.eventType,
              retryCount: failedEvent.retryCount,
              retryLimit,
            });
          }
        } catch (stateErr) {
          this.logger.error('Fatal database error during failure state transitions', stateErr, {
            eventId: event.id,
          });
        }

        // Propagate failure to scheduling cycle boundary
        throw err;
      }
    }

    this.logger.info('Polling cycle completed successfully', {
      processedCount: events.length,
    });
  }
}
