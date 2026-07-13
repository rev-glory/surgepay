import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';

import { OutboxEvent } from './generated/client';

export interface EventPublisher {
  publish(event: OutboxEvent): Promise<void>;
}

export const EVENT_PUBLISHER = 'EVENT_PUBLISHER';

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
