import { randomUUID } from 'crypto';
import { OutboxStatus } from '../../generated/client';

export class OrderOutboxEventEntity {
  constructor(
    public readonly id: string,
    public readonly eventType: string,
    public readonly payload: Record<string, any>,
    public status: OutboxStatus,
    public readonly createdAt: Date,
    public readonly updatedAt: Date
  ) {}

  static create(params: {
    eventType: string;
    payload: Record<string, any>;
  }): OrderOutboxEventEntity {
    return new OrderOutboxEventEntity(
      randomUUID(),
      params.eventType,
      params.payload,
      OutboxStatus.PENDING,
      new Date(),
      new Date()
    );
  }
}
