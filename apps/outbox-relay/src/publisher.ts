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
}
