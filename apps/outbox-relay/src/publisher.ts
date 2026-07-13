import { Injectable } from '@nestjs/common';

import {
  EventSerializer,
  KafkaEventProducer,
  LoggerService,
  TOPIC_REGISTRY,
} from '@surgepay/common';
import type { BaseEventEnvelope } from '@surgepay/events';

import type { OutboxEvent } from './generated/client';

export interface EventPublisher {
  publish(event: OutboxEvent): Promise<void>;
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

  async publish(event: OutboxEvent): Promise<void> {
    // Log delegation only. Do not claim durable publication to Kafka.
    this.logger.info('Event delegated to publisher boundary placeholder', {
      eventId: event.id,
      eventType: event.eventType,
      aggregateId: event.aggregateId,
      correlationId: event.correlationId,
      requestId: event.requestId,
      causationId: event.causationId,
    });
  }
}

@Injectable()
export class KafkaOutboxPublisher implements EventPublisher {
  constructor(private readonly producer: KafkaEventProducer) {}

  async publish(event: OutboxEvent): Promise<void> {
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

    let serialized: Buffer;
    try {
      serialized = EventSerializer.serialize(envelope);
    } catch (err) {
      throw new OutboxPublicationException(
        `Serialization failed: ${err instanceof Error ? err.message : String(err)}`,
        {
          eventId: event.id,
          eventType: event.eventType,
          correlationId: event.correlationId,
          topic,
        },
        err,
      );
    }

    try {
      // Kafka partition key decision using aggregateId (representing paymentId)
      await this.producer.publish(topic, event.aggregateId, serialized);
    } catch (err) {
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
