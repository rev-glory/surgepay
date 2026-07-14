import { Injectable } from '@nestjs/common';

import {
  KafkaEventProducer,
  LoggerService,
  TOPIC_REGISTRY,
} from '@surgepay/common';
import type { BaseEventEnvelope } from '@surgepay/events';

import type { OutboxEvent } from './generated/client';

export interface EventPublisher {
  publish(event: OutboxEvent): Promise<{ partition: number; offset: string }>;
  publishBatch(events: OutboxEvent[]): Promise<{ id: string; partition: number; offset: string }[]>;
}

export const EVENT_PUBLISHER = 'EVENT_PUBLISHER';

export class EnvelopeMismatchException extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EnvelopeMismatchException';
  }
}

export class OutboxPublicationException extends Error {
  constructor(
    message: string,
    public readonly context: {
      eventId: string;
      eventType: string;
      correlationId: string;
      topic: string;
    },
    public readonly originalError?: unknown,
  ) {
    super(message);
    this.name = 'OutboxPublicationException';
  }
}

@Injectable()
export class ConsoleEventPublisher implements EventPublisher {
  constructor(private readonly logger: LoggerService) {
    this.logger.setContext('ConsoleEventPublisher');
  }

  async publish(event: OutboxEvent): Promise<{ partition: number; offset: string }> {
    // Log delegation only. Do not claim durable publication to Kafka.
    this.logger.info('Event delegated to publisher boundary placeholder', {
      eventId: event.id,
      eventType: event.eventType,
      aggregateId: event.aggregateId,
      correlationId: event.correlationId,
      requestId: event.requestId,
      causationId: event.causationId,
    });
    return { partition: 0, offset: '0' };
  }

  async publishBatch(events: OutboxEvent[]): Promise<{ id: string; partition: number; offset: string }[]> {
    const results = [];
    for (const event of events) {
      this.logger.info('Event delegated to publisher boundary placeholder (batch)', {
        eventId: event.id,
        eventType: event.eventType,
        aggregateId: event.aggregateId,
        correlationId: event.correlationId,
        requestId: event.requestId,
        causationId: event.causationId,
      });
      results.push({ id: event.id, partition: 0, offset: '0' });
    }
    return results;
  }
}

@Injectable()
export class KafkaOutboxPublisher implements EventPublisher {
  constructor(private readonly producer: KafkaEventProducer) {}

  async publish(event: OutboxEvent): Promise<{ partition: number; offset: string }> {
    const envelope = event.payload as unknown as BaseEventEnvelope<unknown>;

    if (!envelope || typeof envelope !== 'object') {
      throw new EnvelopeMismatchException('Outbox event payload is not a valid object');
    }

    const topic = TOPIC_REGISTRY[event.eventType];
    if (!topic) {
      throw new OutboxPublicationException(
        `No topic mapping found for event type: ${event.eventType}`,
        {
          eventId: event.id,
          eventType: event.eventType,
          correlationId: event.correlationId,
          topic: '',
        },
      );
    }

    try {
      // Kafka partition key decision using aggregateId (representing paymentId)
      const metadata = await this.producer.publish(
        topic,
        event.aggregateId,
        envelope,
        (event.traceHeaders as Record<string, string>) || undefined,
      );
      const record = metadata[0];
      if (!record) {
        throw new Error('Kafka broker returned empty metadata array');
      }
      return {
        partition: record.partition,
        offset: record.offset ?? '0',
      };
    } catch (err) {
      // Catch validation or serialization exceptions thrown by EventSerializer internally
      const isSerializationErr =
        err instanceof Error &&
        (err.name === 'SerializationException' ||
          err.name === 'MalformedEventEnvelopeException' ||
          err.name === 'UnsupportedEventVersionException');

      if (isSerializationErr) {
        throw new OutboxPublicationException(
          `Serialization failed: ${(err as Error).message}`,
          {
            eventId: event.id,
            eventType: event.eventType,
            correlationId: event.correlationId,
            topic,
          },
          err,
        );
      }

      throw new OutboxPublicationException(
        `Kafka publish failed: ${err instanceof Error ? err.message : String(err)}`,
        {
          eventId: event.id,
          eventType: event.eventType,
          correlationId: event.correlationId,
          topic,
        },
        err,
      );
    }
  }

  async publishBatch(events: OutboxEvent[]): Promise<{ id: string; partition: number; offset: string }[]> {
    const items = events.map((event) => {
      const envelope = event.payload as unknown as BaseEventEnvelope<unknown>;
      if (!envelope || typeof envelope !== 'object') {
        throw new EnvelopeMismatchException('Outbox event payload is not a valid object');
      }
      const topic = TOPIC_REGISTRY[event.eventType];
      if (!topic) {
        throw new OutboxPublicationException(
          `No topic mapping found for event type: ${event.eventType}`,
          {
            eventId: event.id,
            eventType: event.eventType,
            correlationId: event.correlationId,
            topic: '',
          },
        );
      }
      return {
        topic,
        key: event.aggregateId,
        event: envelope,
        headers: (event.traceHeaders as Record<string, string>) || undefined,
        originalEvent: event,
      };
    });

    try {
      const metadataList = await this.producer.publishBatch(
        items.map(item => ({
          topic: item.topic,
          key: item.key,
          event: item.event,
          headers: item.headers,
        }))
      );

      return metadataList.map((meta) => {
        const originalEvent = events.find((e) => {
          const envelope = e.payload as any;
          return envelope && envelope.eventId === meta.eventId;
        });
        return {
          id: originalEvent ? originalEvent.id : meta.eventId,
          partition: meta.partition,
          offset: meta.offset ?? '0',
        };
      });
    } catch (err) {
      const firstItem = items[0];
      throw new OutboxPublicationException(
        `Kafka batch publish failed: ${err instanceof Error ? err.message : String(err)}`,
        {
          eventId: firstItem ? firstItem.originalEvent.id : 'unknown',
          eventType: firstItem ? firstItem.originalEvent.eventType : 'unknown',
          correlationId: firstItem ? firstItem.originalEvent.correlationId : 'unknown',
          topic: firstItem ? firstItem.topic : 'unknown',
        },
        err,
      );
    }
  }
}
