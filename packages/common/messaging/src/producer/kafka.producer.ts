import { Inject, Injectable } from '@nestjs/common';
import { context, propagation, SpanKind, SpanStatusCode,trace } from '@opentelemetry/api';
import { CompressionTypes, Producer, RecordMetadata } from 'kafkajs';

import { LoggerService, MetricsService } from '@surgepay/common';
import { BaseEventEnvelope } from '@surgepay/events';

import { EVENT_SERIALIZER, KAFKA_PRODUCER, KAFKA_PRODUCER_OPTIONS } from '../kafka.tokens';
import { Serializer } from '../serializer/serializer.interface';
import { createKafkaProducer } from './producer.factory';
import { IProducer } from './producer.interface';
import { KafkaProducerOptions } from './producer.options';

@Injectable()
export class KafkaProducer implements IProducer {
  private connected = false;
  private rawProducerInstance: Producer;

  constructor(
    @Inject(KAFKA_PRODUCER) private readonly initialRawProducer: Producer,
    @Inject(KAFKA_PRODUCER_OPTIONS) private readonly options: KafkaProducerOptions,
    @Inject(EVENT_SERIALIZER) private readonly serializer: Serializer,
    @Inject(LoggerService) private readonly logger: LoggerService,
    private readonly metrics: MetricsService,
  ) {
    this.logger.setContext('KafkaProducer');
    this.rawProducerInstance = this.initialRawProducer;
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    this.logger.info('Connecting to Kafka broker...');
    // Recreate the raw producer to reset sequence numbering history and dynamic connection pool
    this.rawProducerInstance = createKafkaProducer(this.options);
    await this.rawProducerInstance.connect();
    this.connected = true;
    this.logger.info('Kafka producer connected successfully.');
  }

  async disconnect(): Promise<void> {
    if (!this.connected) return;
    this.logger.info('Disconnecting from Kafka broker...');
    await this.rawProducerInstance.disconnect();
    this.connected = false;
    this.logger.info('Kafka producer disconnected successfully.');
  }

  isReady(): boolean {
    return this.connected;
  }

  async publish<T = any>(topic: string, event: BaseEventEnvelope<T>): Promise<RecordMetadata[]> {
    if (!this.connected) {
      throw new Error('Kafka producer is not connected');
    }

    const value = this.serializer.serialize(event);
    const startTime = Date.now();

    const tracer = trace.getTracer('@surgepay/common-messaging');
    const span = tracer.startSpan(`${topic} publish`, {
      kind: SpanKind.PRODUCER,
      attributes: {
        'messaging.system': 'kafka',
        'messaging.destination.name': topic,
        'messaging.operation': 'publish',
        'messaging.kafka.client_id': event.producer,
      },
    });

    return context.with(trace.setSpan(context.active(), span), async () => {
      try {
        this.logger.debug('Publishing message to Kafka', {
          topic,
          eventId: event.eventId,
          eventType: event.eventType,
          correlationId: event.correlationId,
        });

        const otelHeaders: Record<string, string> = {};
        propagation.inject(context.active(), otelHeaders);

        const metadata = await this.rawProducerInstance.send({
          topic,
          acks: -1, // all acknowledgements
          compression: CompressionTypes.GZIP,
          messages: [
            {
              key: event.eventId,
              value,
              headers: {
                correlationId: event.correlationId,
                causationId: event.causationId,
                requestId: event.requestId,
                ...otelHeaders,
              },
            },
          ],
        });

        const duration = Date.now() - startTime;
        this.metrics.incrementPublished(event.producer, event.eventType);
        this.metrics.recordPublishDuration(event.producer, event.eventType, duration);

        this.logger.info('Successfully published message to Kafka', {
          topic,
          eventId: event.eventId,
          eventType: event.eventType,
          correlationId: event.correlationId,
          durationMs: duration,
        });

        return metadata;
      } catch (err) {
        const duration = Date.now() - startTime;
        this.metrics.incrementPublishFailures(event.producer, event.eventType);

        this.logger.error('Failed to publish message to Kafka', err, {
          topic,
          eventId: event.eventId,
          eventType: event.eventType,
          correlationId: event.correlationId,
          durationMs: duration,
        });

        span.recordException(err as Error);
        span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
        throw err;
      } finally {
        span.end();
      }
    });
  }

  async publishBatch(messages: Array<{ topic: string; event: BaseEventEnvelope<any> }>): Promise<RecordMetadata[]> {
    if (!this.connected) {
      throw new Error('Kafka producer is not connected');
    }

    if (messages.length === 0) return [];

    const startTime = Date.now();

    const grouped = new Map<string, Array<{ topic: string; event: BaseEventEnvelope<any> }>>();
    for (const msg of messages) {
      if (!grouped.has(msg.topic)) {
        grouped.set(msg.topic, []);
      }
      grouped.get(msg.topic)!.push(msg);
    }

    const tracer = trace.getTracer('@surgepay/common-messaging');
    const span = tracer.startSpan('publishBatch', {
      kind: SpanKind.PRODUCER,
      attributes: {
        'messaging.system': 'kafka',
        'messaging.operation': 'publish',
        'messaging.batch.size': messages.length,
      },
    });

    return context.with(trace.setSpan(context.active(), span), async () => {
      try {
        const topicMessages = Array.from(grouped.entries()).map(([topic, msgs]) => {
          return {
            topic,
            messages: msgs.map((m) => {
              const value = this.serializer.serialize(m.event);
              const otelHeaders: Record<string, string> = {};
              propagation.inject(context.active(), otelHeaders);

              return {
                key: m.event.eventId,
                value,
                headers: {
                  correlationId: m.event.correlationId,
                  causationId: m.event.causationId,
                  requestId: m.event.requestId,
                  ...otelHeaders,
                },
              };
            }),
          };
        });

        const metadata = await this.rawProducerInstance.sendBatch({
          topicMessages,
          acks: -1,
          compression: CompressionTypes.GZIP,
        });

        const duration = Date.now() - startTime;

        for (const msg of messages) {
          this.metrics.incrementPublished(msg.event.producer, msg.event.eventType);
          this.metrics.recordPublishDuration(msg.event.producer, msg.event.eventType, duration);
        }

        this.logger.info('Successfully published message batch to Kafka', {
          batchSize: messages.length,
          durationMs: duration,
        });

        return metadata;
      } catch (err) {
        const duration = Date.now() - startTime;
        for (const msg of messages) {
          this.metrics.incrementPublishFailures(msg.event.producer, msg.event.eventType);
        }

        this.logger.error('Failed to publish message batch to Kafka', err, {
          batchSize: messages.length,
          durationMs: duration,
        });

        span.recordException(err as Error);
        span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
        throw err;
      } finally {
        span.end();
      }
    });
  }
}
