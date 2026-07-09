import { Injectable } from '@nestjs/common';
import { LoggerService, RequestContext } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { Poller } from './poller';
import { PrismaService } from './prisma.service';
import { OutboxPublisher } from './publisher';
import { RelayMetrics } from './metrics.service';

@Injectable()
export class RelayService {
  constructor(
    private readonly prismaService: PrismaService,
    private readonly poller: Poller,
    private readonly publisher: OutboxPublisher,
    private readonly configService: ConfigService,
    private readonly logger: LoggerService,
    private readonly metrics: RelayMetrics,
  ) {
    this.logger.setContext('RelayService');
  }

  /**
   * Runs a single polling and processing cycle inside a database transaction.
   * Acquires row locks using SKIP LOCKED and invokes the publisher for each pending event.
   */
  async runOnce(): Promise<void> {
    const startTime = Date.now();
    const batchSize = this.configService.outbox.batchSize;
    const publishTimeout = this.configService.outbox.publishTimeout;

    this.logger.debug('Starting outbox relay polling cycle', { batchSize });

    let processedCount = 0;

    try {
      // Execute the entire polling and publishing inside a Prisma transaction
      // to keep row locks active while the publisher runs.
      await this.prismaService.client.$transaction(
        async (tx) => {
          const events = await this.poller.pollPending(tx, batchSize);

          if (!events || events.length === 0) {
            return;
          }

          processedCount = events.length;
          this.logger.info('Discovered pending outbox events', { count: events.length });

          for (const event of events) {
            // Propagate Request ID and Correlation ID context in all log statements using RequestContext
            await RequestContext.run(
              {
                requestId: event.requestId,
                correlationId: event.correlationId,
                eventId: event.id,
              },
              async () => {
                this.logger.info('Processing outbox event', {
                  eventId: event.id,
                  eventType: event.eventType,
                });

                try {
                  await this.publisher.publish(event);

                  const lag = Date.now() - event.createdAt.getTime();
                  this.metrics.recordOutboxLag(event.createdAt);
                  this.metrics.recordPublishSuccess(event.eventType);

                  this.logger.info('Successfully processed and published outbox event', {
                    eventId: event.id,
                    eventType: event.eventType,
                    publishLagMs: lag,
                  });
                } catch (err) {
                  this.metrics.recordPublishFailure(event.eventType, true);
                  this.logger.error('Failed to publish outbox event', err, {
                    eventId: event.id,
                    eventType: event.eventType,
                  });
                  // Re-throw to rollback transaction and release locks for other workers to retry
                  throw err;
                }
              },
            );
          }
        },
        {
          timeout: publishTimeout,
        },
      );
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
