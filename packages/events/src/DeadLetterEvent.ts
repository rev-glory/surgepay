import { BaseEvent } from './BaseEvent';
import { EventEnvelope } from './EventEnvelope';

export interface DeadLetterPayload {
  originalEvent: EventEnvelope;
  consumer: string;
  retryCount: number;
  failureReason: string;
  failedAt: string;
}

export class DeadLetterEvent implements BaseEvent<DeadLetterPayload> {
  public readonly eventType = 'DeadLetterEvent';
  public readonly version = 1;

  constructor(public readonly payload: DeadLetterPayload) {}
}
