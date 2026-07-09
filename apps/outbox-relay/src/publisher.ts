import { Injectable } from '@nestjs/common';
import { LoggerService } from '@surgepay/common';

import { OutboxEvent } from '../../payment-service/src/generated/client';

export abstract class OutboxPublisher {
  abstract publish(event: OutboxEvent): Promise<void>;
}

@Injectable()
export class ConsolePublisher implements OutboxPublisher {
  constructor(private readonly logger: LoggerService) {
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
