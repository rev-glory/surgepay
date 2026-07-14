import { type OnModuleDestroy, type OnModuleInit, Optional } from '@nestjs/common';
import {
  context,
  propagation,
  type Span,
  SpanKind,
  SpanStatusCode,
  trace,
} from '@opentelemetry/api';
import { type Consumer, Kafka } from 'kafkajs';

import type { ConfigService } from '@surgepay/config';
import type { BaseEventEnvelope } from '@surgepay/events';

import type { LoggerService } from '../logger';
import { MetricsService } from '../observability/prometheus-metrics.service';
import { DEAD_LETTER_EVENT_TYPE, type DeadLetterRecord } from './dead-letter.types';
import type { BaseInboxRepository } from './inbox.repository';
import type { KafkaEventProducer } from './producer';
import { EventSerializer } from './serializer';
import { resolveDlqTopic } from './topics';
import { kafkaTextMapGetter } from './tracing/kafka-trace-carrier';

export class EventProcessingInProgressException extends Error {
  constructor(eventId: string, consumer: string) {
    super(`Event ${eventId} is currently being processed by consumer ${consumer}.`);
    this.name = 'EventProcessingInProgressException';
  }
}

function isPrismaUniqueConstraintError(err: unknown): boolean {
  return (
    err !== null &&
    typeof err === 'object' &&
    'code' in err &&
    (err as { code: string }).code === 'P2002'
  );
}

export abstract class BaseKafkaConsumer implements OnModuleInit, OnModuleDestroy {
  protected readonly kafka: Kafka;
  protected consumer!: Consumer;
  protected abstract readonly topic: string;
  protected abstract readonly groupId: string;
  protected abstract readonly inboxRepository: BaseInboxRepository;
  private dlqSyncInterval?: NodeJS.Timeout;

  constructor(
    protected readonly config: ConfigService,
    protected readonly logger: LoggerService,
    protected readonly eventProducer: KafkaEventProducer,
    @Optional() protected readonly metricsService?: MetricsService,
  ) {
    const kafkaConfig = this.config.kafka;
    this.kafka = new Kafka({
      clientId: `${kafkaConfig.clientId}-${this.constructor.name.toLowerCase()}`,
      brokers: kafkaConfig.brokers,
      ssl: kafkaConfig.ssl,
      sasl: kafkaConfig.sasl
        ? {
            mechanism: 'plain',
            username: process.env.KAFKA_SASL_USERNAME || '',
            password: process.env.KAFKA_SASL_PASSWORD || '',
          }
        : undefined,
    });
  }

  protected abstract handleEvent(envelope: BaseEventEnvelope<unknown>): Promise<void>;

  async onModuleInit(): Promise<void> {
    this.logger.setContext(this.constructor.name);
    this.logger.info(`Initializing consumer for topic ${this.topic}...`);

    this.consumer = this.kafka.consumer({
      groupId: this.groupId,
      allowAutoTopicCreation: false,
    });

    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: true });

    // Sync DLQ depth baseline on startup
    const serviceName = this.config.logging?.serviceName || 'unknown-service';
    const syncDlqDepth = async (): Promise<void> => {
      try {
        const depth = await this.inboxRepository.countDlqDepth(this.groupId);
        this.metricsService?.setInboxDlqDepth(serviceName, this.groupId, depth);
      } catch (err) {
        this.logger.error('Failed to sync inbox DLQ depth gauge', err as Error);
      }
    };
    await syncDlqDepth();
    this.dlqSyncInterval = setInterval(() => {
      syncDlqDepth().catch(() => {});
    }, 15000);

    await this.consumer.run({
      autoCommit: false, // Explicitly disable autoCommit
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) {
          this.logger.warn('Received empty message payload (tombstone), skipping', {
            topic,
            partition,
            offset: message.offset,
          });
          return;
        }

        const headers = message.headers || {};
        let activeContext = context.active();
        let span: Span | undefined = undefined;

        try {
          const parentContext = propagation.extract(context.active(), headers, kafkaTextMapGetter);
          const tracer = trace.getTracer('surgepay-messaging');
          span = tracer.startSpan(
            `${topic} process`,
            {
              kind: SpanKind.CONSUMER,
              attributes: {
                'messaging.system': 'kafka',
                'messaging.destination.name': topic,
                'messaging.destination_kind': 'topic',
                'messaging.operation': 'process',
                'messaging.kafka.consumer.group': this.groupId,
              },
            },
            parentContext,
          );
          activeContext = trace.setSpan(parentContext, span);
        } catch (traceErr) {
          this.logger.warn('Telemetry tracing extraction failed, degrading gracefully', {
            error: (traceErr as Error).message,
          });
        }

        const runConsumer = async (): Promise<void> => {
          let envelope: BaseEventEnvelope<unknown>;
          try {
            // 1. Deserialization & Validation
            try {
              envelope = EventSerializer.deserialize(message.value!);
              if (span) {
                span.setAttributes({
                  'messaging.message.id': envelope.eventId,
                  'messaging.correlation_id': envelope.correlationId,
                  'messaging.causation_id': envelope.causationId,
                  'messaging.event_type': envelope.eventType,
                });
              }
            } catch (err) {
              this.logger.error('Failed to deserialize/validate event envelope', err as Error, {
                topic,
                partition,
                offset: message.offset,
              });
              throw err;
            }

            // Setup logger context with Correlation ID and Event ID
            const logContext = {
              eventId: envelope.eventId,
              eventType: envelope.eventType,
              correlationId: envelope.correlationId,
              causationId: envelope.causationId,
              sagaId: envelope.sagaId,
              topic,
              partition,
              offset: message.offset,
            };

            const startTime = Date.now();
            this.metricsService?.recordConsumeAttempt(
              serviceName,
              envelope.eventType,
              this.groupId,
            );

            this.logger.info(`Received event ${envelope.eventType}`, logContext);

            // 2. Check duplicate state
            const existing = await this.inboxRepository.findByEventIdAndConsumer(
              envelope.eventId,
              this.groupId,
            );

            if (existing) {
              if (existing.status === 'PROCESSED' || existing.status === 'DLQ_SENT') {
                this.logger.info(`Duplicate event delivery skipped (already ${existing.status})`, {
                  ...logContext,
                  duplicate: true,
                  inboxStatus: existing.status,
                });
                if (span) {
                  span.setAttribute('inbox.duplicate', true);
                  span.setAttribute('inbox.status', existing.status);
                  span.setStatus({ code: SpanStatusCode.OK });
                }
                this.metricsService?.recordDuplicateSkip(
                  serviceName,
                  envelope.eventType,
                  this.groupId,
                );
                await this.commitOffset(topic, partition, (BigInt(message.offset) + 1n).toString());
                return;
              }

              if (existing.status === 'PROCESSING') {
                this.logger.warn('Duplicate event delivery skipped - processing in progress', {
                  ...logContext,
                  duplicate: true,
                  inboxStatus: existing.status,
                });
                throw new EventProcessingInProgressException(envelope.eventId, this.groupId);
              }

              // If status is RECEIVED, RETRYING, or FAILED, we proceed to transition and process it
            } else {
              // First time delivery, record as RECEIVED
              try {
                await this.inboxRepository.recordReceived(envelope, this.groupId);
                this.metricsService?.recordInboxReceived(
                  serviceName,
                  this.groupId,
                  envelope.eventType,
                );
                this.logger.info(
                  `Durable Inbox persistence succeeded for event ${envelope.eventId}`,
                  logContext,
                );
              } catch (err) {
                if (isPrismaUniqueConstraintError(err)) {
                  this.logger.info(
                    'Duplicate event delivery detected via DB constraint (concurrent insert)',
                    {
                      ...logContext,
                      duplicate: true,
                    },
                  );

                  // Re-evaluate the status of the record in DB
                  const collided = await this.inboxRepository.findByEventIdAndConsumer(
                    envelope.eventId,
                    this.groupId,
                  );
                  if (collided) {
                    if (collided.status === 'PROCESSED' || collided.status === 'DLQ_SENT') {
                      if (span) {
                        span.setAttribute('inbox.duplicate', true);
                        span.setAttribute('inbox.status', collided.status);
                        span.setStatus({ code: SpanStatusCode.OK });
                      }
                      this.metricsService?.recordDuplicateSkip(
                        serviceName,
                        envelope.eventType,
                        this.groupId,
                      );
                      await this.commitOffset(
                        topic,
                        partition,
                        (BigInt(message.offset) + 1n).toString(),
                      );
                      return;
                    }
                    if (collided.status === 'PROCESSING') {
                      throw new EventProcessingInProgressException(envelope.eventId, this.groupId);
                    }
                  }
                } else {
                  this.logger.error(
                    `Database write failed for event ${envelope.eventId}`,
                    err as Error,
                    logContext,
                  );
                  throw err;
                }
              }
            }

            // 3. Atomically transition from (RECEIVED, RETRYING, FAILED) to PROCESSING to claim lock
            const transitioned = await this.inboxRepository.transitionStatus(
              envelope.eventId,
              this.groupId,
              ['RECEIVED', 'RETRYING', 'FAILED'],
              'PROCESSING',
            );

            if (!transitioned) {
              this.logger.info(
                'Duplicate event delivery skipped (concurrency lock acquired by another worker)',
                {
                  ...logContext,
                  duplicate: true,
                },
              );
              // Query DB again to see if it is PROCESSING, PROCESSED, or DLQ_SENT
              const current = await this.inboxRepository.findByEventIdAndConsumer(
                envelope.eventId,
                this.groupId,
              );
              if (current && (current.status === 'PROCESSED' || current.status === 'DLQ_SENT')) {
                if (span) {
                  span.setAttribute('inbox.duplicate', true);
                  span.setAttribute('inbox.status', current.status);
                  span.setStatus({ code: SpanStatusCode.OK });
                }
                this.metricsService?.recordDuplicateSkip(
                  serviceName,
                  envelope.eventType,
                  this.groupId,
                );
                await this.commitOffset(topic, partition, (BigInt(message.offset) + 1n).toString());
                return;
              }
              throw new EventProcessingInProgressException(envelope.eventId, this.groupId);
            }

            this.logger.info(
              `Acquired processing lock for event ${envelope.eventId}, executing handler`,
              logContext,
            );

            // 4. Execute the business handler
            try {
              await this.handleEvent(envelope);

              // Mark as PROCESSED
              await this.inboxRepository.updateStatus(envelope.eventId, this.groupId, 'PROCESSED');
              this.metricsService?.recordInboxProcessed(
                serviceName,
                this.groupId,
                envelope.eventType,
              );

              const durationMs = Date.now() - startTime;
              this.metricsService?.recordHandlerDuration(
                serviceName,
                envelope.eventType,
                this.groupId,
                'success',
                durationMs,
              );

              // Commit partition offset
              await this.commitOffset(topic, partition, (BigInt(message.offset) + 1n).toString());
              this.logger.info(
                `Successfully processed event ${envelope.eventId} and committed offset`,
                logContext,
              );
            } catch (handlerErr: unknown) {
              const error = handlerErr as Error;
              this.logger.error(
                `Handler execution failed for event ${envelope.eventId}`,
                error,
                logContext,
              );

              const durationMs = Date.now() - startTime;
              this.metricsService?.recordHandlerDuration(
                serviceName,
                envelope.eventType,
                this.groupId,
                'failure',
                durationMs,
              );
              this.metricsService?.recordHandlerFailure(
                serviceName,
                envelope.eventType,
                this.groupId,
              );

              // Transition back to RETRYING or FAILED depending on retry limits
              const currentRecord = await this.inboxRepository.findByEventIdAndConsumer(
                envelope.eventId,
                this.groupId,
              );
              const currentRetry = currentRecord?.retryCount ?? 0;
              const limit = this.config.kafka.consumerRetryLimit;
              const newRetryCount = currentRetry + 1;

              if (newRetryCount <= limit) {
                // Below retry limit: transition to RETRYING and re-throw
                this.logger.info(
                  `Event ${envelope.eventId} is below retry limit (${newRetryCount}/${limit}), transitioning to RETRYING`,
                  logContext,
                );
                await this.inboxRepository.updateStatus(
                  envelope.eventId,
                  this.groupId,
                  'RETRYING',
                  newRetryCount,
                );
                throw handlerErr;
              } else {
                // Bounded retry limit exhausted: publish to DLQ and transition to DLQ_SENT
                this.logger.warn(
                  `Event ${envelope.eventId} exhausted retries (${newRetryCount}/${limit}), forwarding to DLQ`,
                  logContext,
                );

                const dlqTopic = resolveDlqTopic();
                const dlqPayload: DeadLetterRecord = {
                  originalEvent: envelope,
                  failureReason: error.message || String(error),
                  retryCount: newRetryCount,
                  consumer: this.groupId,
                  failedAt: new Date().toISOString(),
                  dlqTopic,
                };

                const dlqEnvelope = {
                  eventId: envelope.eventId, // Preserve original eventId
                  eventType: DEAD_LETTER_EVENT_TYPE,
                  correlationId: envelope.correlationId,
                  causationId: envelope.eventId,
                  sagaId: envelope.sagaId,
                  timestamp: new Date().toISOString(),
                  version: 1,
                  payload: dlqPayload,
                };

                try {
                  await this.eventProducer.publish(dlqTopic, envelope.eventId, dlqEnvelope);
                  this.logger.info(
                    `DLQ publication succeeded for event ${envelope.eventId} to topic ${dlqTopic}`,
                    logContext,
                  );

                  // Update inbox status to DLQ_SENT
                  await this.inboxRepository.updateStatus(
                    envelope.eventId,
                    this.groupId,
                    'DLQ_SENT',
                    newRetryCount,
                  );
                  this.metricsService?.recordInboxDlqEvent(
                    serviceName,
                    this.groupId,
                    envelope.eventType,
                  );

                  // Sync the DLQ depth gauge immediately from the DB
                  await syncDlqDepth();

                  // Commit partition offset
                  await this.commitOffset(
                    topic,
                    partition,
                    (BigInt(message.offset) + 1n).toString(),
                  );
                  this.logger.info(
                    `Inbox marked DLQ_SENT and offset committed for event ${envelope.eventId}`,
                    logContext,
                  );
                } catch (dlqErr: unknown) {
                  const error = dlqErr as Error;
                  this.logger.error(
                    `DLQ publication failed for event ${envelope.eventId}`,
                    error,
                    logContext,
                  );

                  // Transition to FAILED so it is retried next time (offset remains uncommitted)
                  await this.inboxRepository.updateStatus(
                    envelope.eventId,
                    this.groupId,
                    'FAILED',
                    newRetryCount,
                  );

                  throw dlqErr;
                }
              }
            }

            if (span) {
              span.setStatus({ code: SpanStatusCode.OK });
            }
          } catch (err) {
            if (span) {
              span.recordException(err as Error);
              span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
            }
            throw err;
          } finally {
            if (span) {
              span.end();
            }
          }
        };

        await context.with(activeContext, runConsumer);
      },
    });

    this.logger.info(`Consumer started and running on topic ${this.topic}.`);
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info(`Disconnecting consumer from topic ${this.topic}...`);
    if (this.dlqSyncInterval) {
      clearInterval(this.dlqSyncInterval);
    }
    if (this.consumer) {
      await this.consumer.disconnect();
    }
    this.logger.info('Consumer disconnected.');
  }

  /**
   * Helper function to manually commit partition offsets
   * (Available for invocation starting in Commit 6)
   */
  async commitOffset(topic: string, partition: number, offset: string): Promise<void> {
    await this.consumer.commitOffsets([
      {
        topic,
        partition,
        offset,
      },
    ]);
  }
}
