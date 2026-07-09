import { InboxStatus } from './inbox-status.enum';

export interface InboxEvent<TPayload = unknown> {
  id: string;
  eventId: string;
  consumer: string;
  eventType: string;
  status: InboxStatus;
  payload: TPayload;
  correlationId: string;
  causationId: string;
  sagaId?: string;
  receivedAt: Date;
  processedAt?: Date;
  retryCount: number;
  failureReason?: string;
  createdAt: Date;
  updatedAt: Date;
}
