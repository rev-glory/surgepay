import { Inject, Injectable, OnApplicationShutdown } from '@nestjs/common';

import { LoggerService, MetricsService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { OutboxPoller } from './poller';
import { EVENT_PUBLISHER, EventPublisher } from './publisher';
import { OutboxRepository } from './repositories/outbox.repository';

@Injectable()
export class OutboxRelayService implements OnApplicationShutdown {
  private activeInFlight = 0;
  private readonly activePromises = new Set<Promise<void>>();

  constructor(
    private readonly poller: OutboxPoller,
    private readonly outboxRepository: OutboxRepository,
    @Inject(EVENT_PUBLISHER) private readonly publisher: EventPublisher,
    private readonly config: ConfigService,
    private readonly logger: LoggerService,
    private readonly metricsService: MetricsService,
  ) {
    this.logger.setContext('OutboxRelayService');
  }

  async onApplicationShutdown(): Promise<void> {
    this.logger.info('Awaiting active in-flight outbox publishing batches to complete...');
    await Promise.all(this.activePromises);
    this.logger.info('All in-flight publishing batches completed.');
  }

  /**
   * Orchestrates a single polling and publishing batch cycle.
   * Coordinates stale claim recovery, claiming eligible events,
   * delegating to the publisher, and committing final states.
   */
  async processBatch(): Promise<void> {
    const startTime = Date.now();
    const staleTimeout = this.config.outbox.staleTimeoutMs;
    const retryLimit = this.config.outbox.retryLimit;
    const batchSize = this.config.outbox.batchSize;
    const maxInFlight = this.config.outbox.maxInFlightMessages;
    const serviceName = this.config.logging.serviceName;

    // Sync outbox gauges at start of cycle
    try {
      const pending = await this.outboxRepository.countPending();
      const failed = await this.outboxRepository.countFailed();
      const published = await this.outboxRepository.countPublished();
      this.metricsService.setOutboxPending(serviceName, pending);
      this.metricsService.setOutboxFailed(serviceName, failed);
      this.metricsService.setOutboxPublished(serviceName, published);
      this.metricsService.setOutboxInFlight(serviceName, this.activeInFlight);
    } catch (gaugeErr) {
      this.logger.error('Failed to sync outbox gauges', gaugeErr as Error);
    }

    // 1. Recover stale claims (PUBLISHING -> FAILED -> RETRYING/FAILED)
    try {
      await this.poller.recoverStale(staleTimeout, retryLimit);
    } catch (staleErr) {
      this.logger.error('Stale recovery failed', staleErr as Error);
    }

    const promises: Promise<void>[] = [];
    let batchesSpawned = 0;
    const maxBatchesPerCycle = 10;

    // 2. Loop to poll and spawn background promises until capacity is saturated
    while (batchesSpawned < maxBatchesPerCycle) {
      const currentInFlight = this.activeInFlight;
      if (currentInFlight >= maxInFlight) {
        this.logger.warn('Relay capacity saturated. Skipping poll to apply back-pressure.', {
          activeInFlight: currentInFlight,
          maxInFlight,
        });
        break;
      }

      const capacity = maxInFlight - currentInFlight;
      const currentBatchLimit = Math.min(batchSize, capacity);

      let events;
      try {
        events = await this.poller.pollPending(currentBatchLimit);
      } catch (pollErr) {
        this.logger.error('Outbox poll pending query failed', pollErr as Error);
        break;
      }

      if (events.length === 0) {
        break;
      }

      const returnedLength = events.length;

      // Increment batch count
      batchesSpawned++;

      this.logger.info('Pending events claimed for publication', {
        count: events.length,
        batchSize: currentBatchLimit,
        activeInFlightBefore: this.activeInFlight,
      });

      this.activeInFlight += events.length;
      this.metricsService.setOutboxInFlight(serviceName, this.activeInFlight);
      this.metricsService.recordOutboxBatchSize(serviceName, events.length);

      const promiseContainer: { promise: Promise<void> | undefined } = { promise: undefined };
      const runPublish = async (): Promise<void> => {
        try {
          const metadataList = await this.publisher.publishBatch(events);

          // Record metrics for each published event
          for (const event of events) {
            const lagMs = Date.now() - event.createdAt.getTime();
            this.metricsService.recordOutboxLag(serviceName, event.eventType, lagMs);
          }

          // Bulk transition to PUBLISHED
          await this.outboxRepository.markPublishedBatch(metadataList);
        } catch (err) {
          this.logger.error('Batch publisher boundary failure. Coordinating rollback transitions.', err, {
            eventCount: events.length,
          });

          try {
            // Bulk transition to FAILED/RETRYING
            const eventIds = events.map(e => e.id);
            await this.outboxRepository.markFailedBatch(eventIds, err instanceof Error ? err.message : String(err), retryLimit);
            
            for (const event of events) {
              this.metricsService.recordPublicationRetry(serviceName, event.eventType);
            }
          } catch (stateErr) {
            this.logger.error('Fatal database error during failure state transitions (batch)', stateErr);
          }
          throw err;
        } finally {
          this.activeInFlight -= events.length;
          this.metricsService.setOutboxInFlight(serviceName, this.activeInFlight);
          this.metricsService.recordOutboxCycleDuration(serviceName, Date.now() - startTime);
          this.activePromises.delete(promiseContainer.promise!);
        }
      };

      promiseContainer.promise = runPublish();
      this.activePromises.add(promiseContainer.promise);
      promises.push(promiseContainer.promise);

      if (returnedLength < currentBatchLimit) {
        break;
      }
    }

    if (promises.length > 0) {
      await Promise.all(promises);
    }
  }
}
