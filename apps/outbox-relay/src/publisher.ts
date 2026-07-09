import { Inject, Injectable } from '@nestjs/common';
import { RecordMetadata } from 'kafkajs';

import { LoggerService } from '@surgepay/common';
import { ProducerService, TOPICS } from '@surgepay/common-messaging';
import { BaseEventEnvelope } from '@surgepay/events';

import { OutboxEvent } from '../../payment-service/src/generated/client';

export abstract class OutboxPublisher {
  abstract publish(event: OutboxEvent): Promise<RecordMetadata[]>;
  abstract publishBatch(events: OutboxEvent[]): Promise<RecordMetadata[]>;
}

@Injectable()
export class ConsolePublisher implements OutboxPublisher {
  constructor(@Inject(LoggerService) private readonly logger: LoggerService) {
    this.logger.setContext('ConsolePublisher');
  }

  async publish(event: OutboxEvent): Promise<RecordMetadata[]> {
    this.logger.info('Simulating publishing outbox event to messaging system', {
      eventId: event.id,
      eventType: event.eventType,
      correlationId: event.correlationId,
      causationId: event.causationId,
      requestId: event.requestId,
    });
    return [
      {
        topicName: event.eventType === 'PaymentInitiated' ? TOPICS.PAYMENTS_INITIATED : TOPICS.SAGA_COMMANDS,
        partition: 0,
        offset: '0',
        errorCode: 0,
      },
    ];
  }

  async publishBatch(events: OutboxEvent[]): Promise<RecordMetadata[]> {
    this.logger.info('Simulating publishing outbox event batch to messaging system', {
      count: events.length,
    });
    return events.map((event) => ({
      topicName: event.eventType === 'PaymentInitiated' ? TOPICS.PAYMENTS_INITIATED : TOPICS.SAGA_COMMANDS,
      partition: 0,
      offset: '0',
      errorCode: 0,
    }));
  }
}

@Injectable()
export class KafkaPublisher implements OutboxPublisher {
  constructor(
    private readonly producerService: ProducerService,
    @Inject(LoggerService) private readonly logger: LoggerService,
  ) {
    this.logger.setContext('KafkaPublisher');
  }

  async publish(event: OutboxEvent): Promise<RecordMetadata[]> {
    // The Outbox row's payload field already contains the pre-serialized event envelope
    // as written by the origin service. We cast and forward it directly.
    const envelope = event.payload as unknown as BaseEventEnvelope<unknown>;

    // Resolve the appropriate topic based on event type
    let topic: string;
    if (event.eventType === 'PaymentInitiated') {
      topic = TOPICS.PAYMENTS_INITIATED;
    } else if (event.eventType.endsWith('Command')) {
      topic = TOPICS.SAGA_COMMANDS;
    } else {
      this.logger.warn('Unknown eventType mapping; defaulting to saga commands topic', {
        eventType: event.eventType,
        eventId: event.id,
      });
      topic = TOPICS.SAGA_COMMANDS;
    }

    this.logger.info('Forwarding event envelope to Kafka', {
      eventId: event.id,
      eventType: event.eventType,
      topic,
    });

    return this.producerService.publish(topic, envelope);
  }

  async publishBatch(events: OutboxEvent[]): Promise<RecordMetadata[]> {
    if (events.length === 0) return [];

    const messages = events.map((event) => {
      const envelope = event.payload as unknown as BaseEventEnvelope<unknown>;
      let topic: string;
      if (event.eventType === 'PaymentInitiated') {
        topic = TOPICS.PAYMENTS_INITIATED;
      } else if (event.eventType.endsWith('Command')) {
        topic = TOPICS.SAGA_COMMANDS;
      } else {
        this.logger.warn('Unknown eventType mapping; defaulting to saga commands topic', {
          eventType: event.eventType,
          eventId: event.id,
        });
        topic = TOPICS.SAGA_COMMANDS;
      }
      return { topic, event: envelope };
    });

    return this.producerService.publishBatch(messages);
  }
}
