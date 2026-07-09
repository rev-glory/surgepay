import { Inject, Injectable } from '@nestjs/common';
import { LoggerService } from '@surgepay/common';
import { BaseEventEnvelope } from '@surgepay/events';
import { ProducerService, TOPICS } from '@surgepay/common-messaging';

import { OutboxEvent } from '../../payment-service/src/generated/client';

export abstract class OutboxPublisher {
  abstract publish(event: OutboxEvent): Promise<void>;
}

@Injectable()
export class ConsolePublisher implements OutboxPublisher {
  constructor(@Inject(LoggerService) private readonly logger: LoggerService) {
    this.logger.setContext('ConsolePublisher');
  }

  async publish(event: OutboxEvent): Promise<void> {
    this.logger.info('Simulating publishing outbox event to messaging system', {
      eventId: event.id,
      eventType: event.eventType,
      correlationId: event.correlationId,
      causationId: event.causationId,
      requestId: event.requestId,
    });
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

  async publish(event: OutboxEvent): Promise<void> {
    // The Outbox row's payload field already contains the pre-serialized event envelope
    // as written by the origin service (e.g. Payment Service). We cast and forward it directly.
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

    await this.producerService.publish(topic, envelope);
  }
}
