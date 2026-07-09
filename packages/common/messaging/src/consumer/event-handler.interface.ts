import { EventEnvelope } from '@surgepay/events';

export interface KafkaEventHandler {
  handle(event: EventEnvelope): Promise<void>;
}
