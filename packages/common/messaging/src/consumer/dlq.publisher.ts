import { Injectable } from '@nestjs/common';
import { EventEnvelope } from '@surgepay/events';
import { ProducerService } from '../producer/producer.service';

export interface DlqPublisher {
  /**
   * Publishes the wrapped DeadLetterEvent envelope to the configured DLQ topic.
   */
  publish(topic: string, dlqEnvelope: EventEnvelope): Promise<void>;
}

@Injectable()
export class KafkaDlqPublisher implements DlqPublisher {
  constructor(private readonly producer: ProducerService) {}

  async publish(topic: string, dlqEnvelope: EventEnvelope): Promise<void> {
    await this.producer.publish(topic, dlqEnvelope);
  }
}
