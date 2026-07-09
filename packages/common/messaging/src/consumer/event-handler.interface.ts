import type { EventEnvelope } from '@surgepay/events';

export interface KafkaEventHandler {
  handle(event: EventEnvelope): Promise<void>;
}
