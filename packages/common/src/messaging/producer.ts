import { Injectable, OnModuleDestroy, OnModuleInit, Optional } from '@nestjs/common';
import {
  context,
  propagation,
  type Span,
  SpanKind,
  SpanStatusCode,
  trace,
} from '@opentelemetry/api';
import { CompressionTypes, Kafka, Producer, RecordMetadata } from 'kafkajs';

import { ConfigService } from '@surgepay/config';
import { BaseEventEnvelope } from '@surgepay/events';

import { LoggerService } from '../logger';
import { MetricsService } from '../observability/prometheus-metrics.service';
import { EventSerializer } from './serializer';

export interface BatchPublishItem {
  topic: string;
  key: string;
  event: BaseEventEnvelope<unknown> & { requestId?: string };
  headers?: Record<string, string>;
}

export interface EventProducer {
  publish(
    topic: string,
    key: string,
    event: BaseEventEnvelope<unknown> & { requestId?: string },
    headers?: Record<string, string>,
  ): Promise<RecordMetadata[]>;
  publishBatch(
    items: BatchPublishItem[],
  ): Promise<(RecordMetadata & { eventId: string })[]>;
}

export const EVENT_PRODUCER = 'EVENT_PRODUCER';

@Injectable()
export class KafkaEventProducer implements EventProducer, OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly producer: Producer;

  constructor(
    private readonly config: ConfigService,
    private readonly logger: LoggerService,
    @Optional() private readonly metricsService?: MetricsService,
  ) {
    this.logger.setContext('KafkaEventProducer');

    const kafkaConfig = this.config.kafka;

    this.kafka = new Kafka({
      clientId: kafkaConfig.clientId,
      brokers: kafkaConfig.brokers,
      ssl: kafkaConfig.ssl,
      sasl: kafkaConfig.sasl
        ? {
            mechanism: 'plain', // fallback sasl config mechanism
            username: process.env.KAFKA_SASL_USERNAME || '',
            password: process.env.KAFKA_SASL_PASSWORD || '',
          }
        : undefined,
    });

    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
      retry: {
        retries: 5,
      },
    });
  }

  async onModuleInit(): Promise<void> {
    this.logger.info('Connecting to Redpanda/Kafka broker...');
    await this.producer.connect();
    this.logger.info('Successfully connected to Redpanda/Kafka broker.');
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Disconnecting from Redpanda/Kafka broker...');
    await this.producer.disconnect();
    this.logger.info('Successfully disconnected from Redpanda/Kafka broker.');
  }

  async publish(
    topic: string,
    key: string,
    event: BaseEventEnvelope<unknown> & { requestId?: string },
    headers?: Record<string, string>,
  ): Promise<RecordMetadata[]> {
    const startTime = Date.now();
    const serviceName = this.config.logging?.serviceName || 'unknown-service';

    let activeContext = context.active();
    let span: Span | undefined = undefined;
    const finalHeaders: Record<string, string | Buffer> = {
      correlationId: event.correlationId,
      causationId: event.causationId,
      requestId: event.requestId || '',
    };

    try {
      const parentContext = headers
        ? propagation.extract(context.active(), headers)
        : context.active();

      const tracer = trace.getTracer('surgepay-messaging');
      span = tracer.startSpan(
        `${topic} send`,
        {
          kind: SpanKind.PRODUCER,
          attributes: {
            'messaging.system': 'kafka',
            'messaging.destination.name': topic,
            'messaging.destination_kind': 'topic',
            'messaging.operation': 'publish',
            'messaging.message.id': event.eventId,
            'messaging.correlation_id': event.correlationId,
            'messaging.causation_id': event.causationId,
            'messaging.event_type': event.eventType,
            'messaging.kafka.message.key': key,
          },
        },
        parentContext,
      );

      activeContext = trace.setSpan(parentContext, span);

      const propagatedHeaders: Record<string, string> = {};
      propagation.inject(activeContext, propagatedHeaders);
      Object.assign(finalHeaders, propagatedHeaders);
    } catch (traceErr) {
      this.logger.warn('Telemetry tracing initialization failed, degrading gracefully', {
        error: (traceErr as Error).message,
      });
    }

    const runPublish = async (): Promise<RecordMetadata[]> => {
      try {
        const value = EventSerializer.serialize(event);
        const metadata = await this.producer.send({
          topic,
          acks: -1,
          compression: CompressionTypes.GZIP,
          messages: [
            {
              key,
              value,
              headers: finalHeaders,
            },
          ],
        });
        if (span) {
          span.setStatus({ code: SpanStatusCode.OK });
        }
        const durationMs = Date.now() - startTime;
        this.metricsService?.recordPublishSuccess(serviceName, event.eventType, durationMs);
        return metadata;
      } catch (err) {
        if (span) {
          span.recordException(err as Error);
          span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
        }
        const durationMs = Date.now() - startTime;
        this.metricsService?.recordPublishFailure(serviceName, event.eventType, durationMs);
        throw err;
      } finally {
        if (span) {
          span.end();
        }
      }
    };

    return context.with(activeContext, runPublish);
  }

  async publishBatch(
    items: BatchPublishItem[],
  ): Promise<(RecordMetadata & { eventId: string })[]> {
    const startTime = Date.now();
    const serviceName = this.config.logging?.serviceName || 'unknown-service';

    const preparedMessages: {
      topic: string;
      originalIndex: number;
      message: {
        key: string;
        value: string | Buffer;
        headers: Record<string, string | Buffer>;
      };
      span?: Span;
      activeContext: any;
    }[] = [];

    const tracer = trace.getTracer('surgepay-messaging');

    let idx = 0;
    for (const item of items) {
      const event = item.event;
      const headers = item.headers;
      
      const finalHeaders: Record<string, string | Buffer> = {
        correlationId: event.correlationId,
        causationId: event.causationId,
        requestId: event.requestId || '',
      };

      let activeContext = context.active();
      let span: Span | undefined = undefined;

      try {
        const parentContext = headers
          ? propagation.extract(context.active(), headers)
          : context.active();

        span = tracer.startSpan(
          `${item.topic} send`,
          {
            kind: SpanKind.PRODUCER,
            attributes: {
              'messaging.system': 'kafka',
              'messaging.destination.name': item.topic,
              'messaging.destination_kind': 'topic',
              'messaging.operation': 'publish',
              'messaging.message.id': event.eventId,
              'messaging.correlation_id': event.correlationId,
              'messaging.causation_id': event.causationId,
              'messaging.event_type': event.eventType,
              'messaging.kafka.message.key': item.key,
            },
          },
          parentContext,
        );

        activeContext = trace.setSpan(parentContext, span);

        const propagatedHeaders: Record<string, string> = {};
        propagation.inject(activeContext, propagatedHeaders);
        Object.assign(finalHeaders, propagatedHeaders);
      } catch (traceErr) {
        this.logger.warn('Telemetry tracing initialization failed, degrading gracefully', {
          error: (traceErr as Error).message,
        });
      }

      preparedMessages.push({
        topic: item.topic,
        originalIndex: idx,
        message: {
          key: item.key,
          value: EventSerializer.serialize(event),
          headers: finalHeaders,
        },
        span,
        activeContext,
      });
      idx++;
    }

    // Group messages by topic
    const messagesByTopic: Record<string, any[]> = {};
    for (const msg of preparedMessages) {
      if (!messagesByTopic[msg.topic]) {
        messagesByTopic[msg.topic] = [];
      }
      messagesByTopic[msg.topic]!.push(msg.message);
    }

    const topicMessages = Object.entries(messagesByTopic).map(([topic, messages]) => ({
      topic,
      messages,
    }));

    try {
      const metadata = await this.producer.sendBatch({
        topicMessages,
        acks: -1,
        compression: CompressionTypes.GZIP,
      });

      // End all spans successfully
      for (const msg of preparedMessages) {
        if (msg.span) {
          msg.span.setStatus({ code: SpanStatusCode.OK });
          msg.span.end();
        }
      }

      const durationMs = Date.now() - startTime;
      // Record success metrics for each event in the batch
      for (const item of items) {
        this.metricsService?.recordPublishSuccess(serviceName, item.event.eventType, durationMs);
      }

      // Map returned metadata back to the items by matching topic name
      return items.map((item) => {
        const topicMeta = metadata.find((m) => m.topicName === item.topic);
        return {
          eventId: item.event.eventId,
          topicName: item.topic,
          partition: topicMeta ? topicMeta.partition : 0,
          offset: topicMeta ? (topicMeta.offset ?? '0') : '0',
          errorCode: topicMeta ? topicMeta.errorCode : 0,
        };
      });
    } catch (err) {
      // End all spans with error status
      for (const msg of preparedMessages) {
        if (msg.span) {
          msg.span.recordException(err as Error);
          msg.span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
          msg.span.end();
        }
      }

      const durationMs = Date.now() - startTime;
      for (const item of items) {
        this.metricsService?.recordPublishFailure(serviceName, item.event.eventType, durationMs);
      }
      throw err;
    }
  }
}
