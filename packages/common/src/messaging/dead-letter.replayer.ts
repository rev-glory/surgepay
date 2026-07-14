import { Injectable } from '@nestjs/common';

import { BaseEventEnvelope } from '@surgepay/events';

import { DeadLetterRecord } from './dead-letter.types';
import { KafkaEventProducer } from './producer';
import { EventSerializer } from './serializer';
import { TOPIC_REGISTRY } from './topics';

@Injectable()
export class DeadLetterReplayer {
  constructor(private readonly producer: KafkaEventProducer) {}

  async replay(record: DeadLetterRecord): Promise<{ success: boolean; error?: string }> {
    try {
      // 1. Validate record contents
      if (!record || !record.originalEvent) {
        return { success: false, error: 'Malformed DLQ record: originalEvent is missing' };
      }

      const envelope = record.originalEvent;

      // 2. Validate envelope structure using Serializer
      let validatedEnvelope: BaseEventEnvelope<unknown>;
      try {
        const serialized = EventSerializer.serialize(envelope);
        validatedEnvelope = EventSerializer.deserialize(serialized);
      } catch (err: unknown) {
        const error = err as Error;
        return { success: false, error: `Invalid original event envelope: ${error.message}` };
      }

      // 3. Resolve the original topic mapping
      const topic = TOPIC_REGISTRY[validatedEnvelope.eventType];
      if (!topic) {
        return {
          success: false,
          error: `Unknown event-to-topic mapping for eventType: ${validatedEnvelope.eventType}`,
        };
      }

      // 4. Republish the original event using its original eventId and key
      const key = validatedEnvelope.eventId;
      await this.producer.publish(topic, key, validatedEnvelope);

      return { success: true };
    } catch (err: unknown) {
      const error = err as Error;
      return { success: false, error: error.message };
    }
  }
}
