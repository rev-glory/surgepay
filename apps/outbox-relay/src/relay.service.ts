import { Injectable } from '@nestjs/common';
import { LoggerService, RequestContext } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { Poller } from './poller';
import { PrismaService } from './prisma.service';
import { OutboxPublisher } from './publisher';
import { RelayMetrics } from './metrics.service';
import { BackpressureController } from './backpressure';

@Injectable()
export class RelayService {
  private readonly backpressure: BackpressureController;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly poller: Poller,
    private readonly publisher: OutboxPublisher,
    private readonly configService: ConfigService,
    private readonly logger: LoggerService,
    private readonly metrics: RelayMetrics,
  ) {
    this.logger.setContext('RelayService');
    const maxInFlight = this.configService.outbox.maxInFlight || 1000;
    this.backpressure = new BackpressureController(maxInFlight);
  }

  private getTopicName(eventType: string): string {
    if (eventType === 'PaymentInitiated') {
      return 'payments.initiated';
    }
    return 'saga.commands';
  }

  /**
   * Runs a single polling and processing cycle.
   * Claims pending/retrying events in one quick transaction, then publishes and transitions state for batches.
   */
  async runOnce(): Promise<void> {
    const startTime = Date.now();
    const batchSize = this.configService.outbox.batchSize;
    const publishTimeout = this.configService.outbox.publishTimeout;

    // Fetch up to 10 batches to process concurrently
    const dbPollLimit = batchSize * 10;

    this.logger.debug('Starting outbox relay polling cycle', { batchSize, dbPollLimit });

    // Track gauge metrics of Outbox database state
    try {
      const counts = await this.prismaService.client.outboxEvent.groupBy({
        by: ['eventType', 'status'],
        _count: {
          _all: true,
        },
      });

      const pendingCounts: Record<string, number> = {};
      const publishedCounts: Record<string, number> = {};
      const failedCounts: Record<string, number> = {};

      for (const item of counts) {
        const et = item.eventType;
        const count = item._count._all;
        if (item.status === 'PUBLISHED') {
          publishedCounts[et] = (publishedCounts[et] || 0) + count;
        } else if (item.status === 'FAILED') {
          failedCounts[et] = (failedCounts[et] || 0) + count;
        } else {
          pendingCounts[et] = (pendingCounts[et] || 0) + count;
        }
      }

      for (const et of Object.keys({ ...pendingCounts, ...publishedCounts, ...failedCounts })) {
        this.metrics.recordPendingCount(et, pendingCounts[et] || 0);
        this.metrics.recordPublishedCount(et, publishedCounts[et] || 0);
        this.metrics.recordFailedCount(et, failedCounts[et] || 0);
      }
    } catch (metricsErr) {
      this.logger.error('Failed to update Outbox gauge metrics', metricsErr);
    }

    let processedCount = 0;

    try {
      // 1. Discover and claim work inside a short transaction, transitioning records to PUBLISHING
      const events = await this.prismaService.client.$transaction(
        async (tx) => {
          const pending = await this.poller.pollPending(tx, dbPollLimit);
          if (!pending || pending.length === 0) {
            return [];
          }

          // Mark status = PUBLISHING to prevent concurrent workers from claiming these records
          await tx.outboxEvent.updateMany({
            where: { id: { in: pending.map((e) => e.id) } },
            data: { status: 'PUBLISHING' },
          });

          return pending;
        },
        {
          timeout: publishTimeout,
        },
      );

      if (!events || events.length === 0) {
        return;
      }

      processedCount = events.length;
      this.logger.info('Discovered and claimed outbox events', { count: events.length });

      // Chunk the claimed events into sub-batches of size batchSize
      const chunks: typeof events[] = [];
      for (let i = 0; i < events.length; i += batchSize) {
        chunks.push(events.slice(i, i + batchSize));
      }

      // Publish chunks concurrently with back-pressure constraints
      const publishPromises = chunks.map(async (subBatch) => {
        await this.backpressure.acquire(subBatch.length);
        this.metrics.setRelayInFlight(this.backpressure.getActiveMessagesCount());

        const subBatchStartTime = Date.now();
        try {
          // 2. Publish Event Batch to Kafka
          const metadataList = await this.publisher.publishBatch(subBatch);

          // 3. Update status to PUBLISHED for all events individually inside a transaction
          await this.prismaService.client.$transaction(async (tx) => {
            for (const event of subBatch) {
              const ack = metadataList.find((m) => m.topicName === this.getTopicName(event.eventType)) || metadataList[0];
              const topic = ack?.topicName ?? this.getTopicName(event.eventType);
              const partition = ack?.partition ?? 0;
              const offset = ack?.offset ?? '0';

              await tx.outboxEvent.update({
                where: { id: event.id },
                data: {
                  status: 'PUBLISHED',
                  publishedAt: new Date(),
                  topic,
                  partition,
                  offset,
                },
              });
            }
          });

          const lag = Date.now() - subBatch[0]!.createdAt.getTime();
          const duration = Date.now() - subBatchStartTime;

          this.metrics.recordRelayBatchSize(subBatch.length);
          this.metrics.recordRelayPublishDuration(duration);

          for (const event of subBatch) {
            this.metrics.recordOutboxLag(event.createdAt);
            this.metrics.recordPublishSuccess(event.eventType);

            this.logger.info('Successfully published outbox event', {
              outboxId: event.id,
              eventId: event.id,
              eventType: event.eventType,
              correlationId: event.correlationId,
              currentState: 'PUBLISHING',
              nextState: 'PUBLISHED',
              retryCount: event.retryCount,
              publishDurationMs: duration,
              publishLagMs: lag,
            });
          }
        } catch (err) {
          const duration = Date.now() - subBatchStartTime;
          const errorMsg = err instanceof Error ? err.message : String(err);

          this.logger.error('Failed to publish outbox event batch, initiating failure transitions', err, {
            batchSize: subBatch.length,
            durationMs: duration,
          });

          for (const event of subBatch) {
            this.metrics.recordPublishFailure(event.eventType, true, event.retryCount + 1);

            try {
              // 4. Persist intermediate state transition: FAILED
              await this.prismaService.client.$transaction(async (tx) => {
                await tx.outboxEvent.update({
                  where: { id: event.id },
                  data: {
                    status: 'FAILED',
                  },
                });
              });

              // 5. Transition to RETRYING, increment retryCount, write lastError, lastAttemptAt
              await this.prismaService.client.$transaction(async (tx) => {
                await tx.outboxEvent.update({
                  where: { id: event.id },
                  data: {
                    status: 'RETRYING',
                    retryCount: { increment: 1 },
                    lastError: errorMsg,
                    lastAttemptAt: new Date(),
                  },
                });
              });

              this.logger.info('Successfully transitioned and committed state to RETRYING', {
                outboxId: event.id,
                eventId: event.id,
                eventType: event.eventType,
                correlationId: event.correlationId,
                currentState: 'PUBLISHING',
                nextState: 'RETRYING',
                retryCount: event.retryCount + 1,
              });
            } catch (dbErr) {
              this.logger.error('Critical database error during failure state transitions persistence', dbErr, {
                outboxId: event.id,
                eventId: event.id,
              });
            }
          }
        } finally {
          this.backpressure.release(subBatch.length);
          this.metrics.setRelayInFlight(this.backpressure.getActiveMessagesCount());
        }
      });

      await Promise.all(publishPromises);
    } catch (err) {
      this.logger.error('Error occurred during outbox relay processing cycle', err);
    } finally {
      const duration = Date.now() - startTime;
      this.metrics.recordPollCycle(duration, processedCount);

      if (processedCount > 0) {
        this.logger.info('Completed outbox relay cycle', {
          processedCount,
          batchSize,
          durationMs: duration,
        });
      } else {
        this.logger.debug('Completed outbox relay cycle - no events to process', {
          processedCount,
          batchSize,
          durationMs: duration,
        });
      }
    }
  }
}
