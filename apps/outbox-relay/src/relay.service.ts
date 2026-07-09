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
   * Runs a single polling and processing cycle.
   * Claims pending/retrying events in one quick transaction, then publishes and transitions state for each event.
   */
  async runOnce(): Promise<void> {
    const startTime = Date.now();
    const batchSize = this.configService.outbox.batchSize;
    const publishTimeout = this.configService.outbox.publishTimeout;

    this.logger.debug('Starting outbox relay polling cycle', { batchSize });

    let processedCount = 0;

    try {
      // 1. Discover and claim work inside a short transaction, transitioning records to PUBLISHING
      const events = await this.prismaService.client.$transaction(
        async (tx) => {
          const pending = await this.poller.pollPending(tx, batchSize);
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

            const eventStartTime = Date.now();

            try {
              // 2. Publish Event to Kafka
              const metadata = await this.publisher.publish(event);

              // 3. Extract Broker ACK metadata directly from the returned RecordMetadata
              const ack = metadata[0];
              const topic = ack?.topicName ?? event.eventType;
              const partition = ack?.partition ?? 0;
              const offset = ack?.offset ?? '0';

              // 4. Update status to PUBLISHED along with broker metadata
              await this.prismaService.client.$transaction(async (tx) => {
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
              });

              const lag = Date.now() - event.createdAt.getTime();
              const duration = Date.now() - eventStartTime;

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
                topic,
                partition,
                offset,
                publishDurationMs: duration,
                publishLagMs: lag,
              });
            } catch (err) {
              const duration = Date.now() - eventStartTime;
              const errorMsg = err instanceof Error ? err.message : String(err);

              this.logger.error('Failed to publish outbox event, initiating failure transitions', err, {
                outboxId: event.id,
                eventId: event.id,
                eventType: event.eventType,
                correlationId: event.correlationId,
                currentState: 'PUBLISHING',
                nextState: 'FAILED',
                retryCount: event.retryCount,
                publishDurationMs: duration,
              });

              this.metrics.recordPublishFailure(event.eventType, true);

              try {
                // 5. Persist intermediate state transition: FAILED
                await this.prismaService.client.$transaction(async (tx) => {
                  await tx.outboxEvent.update({
                    where: { id: event.id },
                    data: {
                      status: 'FAILED',
                    },
                  });
                });

                this.logger.info('Persisted intermediate state FAILED', {
                  outboxId: event.id,
                  eventId: event.id,
                });

                // 6. Transition to RETRYING, increment retryCount, write lastError, lastAttemptAt
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
                  currentState: 'FAILED',
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
          },
        );
      }
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
