import { randomUUID } from 'crypto';

import { OutboxStatus } from '../generated/client';

export class OutboxEventEntity {
  constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly aggregateType: string,
    public readonly eventType: string,
    public readonly payload: Record<string, unknown>,
    public readonly status: OutboxStatus,
    public readonly createdAt: Date,
    public readonly publishedAt: Date | null,
    public readonly retryCount: number,
  ) {}

  static create(params: {
    aggregateId: string;
    aggregateType: string;
    eventType: string;
    payload: Record<string, unknown>;
  }): OutboxEventEntity {
    return new OutboxEventEntity(
      randomUUID(),
      params.aggregateId,
      params.aggregateType,
      params.eventType,
      params.payload,
      OutboxStatus.PENDING,
      new Date(),
      null,
      0,
    );
  }
}
