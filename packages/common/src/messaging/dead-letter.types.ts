import type { BaseEventEnvelope } from '@surgepay/events';

export const DEAD_LETTER_EVENT_TYPE = 'DeadLetterRecord';

export interface DeadLetterRecord {
  originalEvent: BaseEventEnvelope<unknown>;
  failureReason: string;
  retryCount: number;
  consumer: string;
  failedAt: string;
  dlqTopic: string;
}
