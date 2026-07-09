import { BaseEvent } from './BaseEvent';

export interface EventEnvelope<T = unknown> extends BaseEvent<T> {
  eventId: string;
  timestamp: string;
  requestId: string;
  correlationId: string;
  causationId: string;
  sagaId?: string;
  producer: string;
}

export type BaseEventEnvelope<T = unknown> = EventEnvelope<T>;
