import { randomUUID } from 'crypto';

import { context, propagation } from '@opentelemetry/api';

import { OutboxStatus } from '../generated/client';

export class OutboxEventEntity {
  constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly aggregateType: string,
    public readonly eventType: string,
    public readonly payload: Record<string, unknown>,
    public readonly status: OutboxStatus,
    public readonly requestId: string,
    public readonly correlationId: string,
    public readonly causationId: string,
    public readonly createdAt: Date,
    public readonly publishedAt: Date | null,
    public readonly retryCount: number,
    public readonly traceHeaders: Record<string, string> | null = null
  ) {}

  static create(params: {
    aggregateId: string;
    aggregateType: string;
    eventType: string;
    payload: Record<string, unknown>;
    requestId: string;
    correlationId: string;
    causationId: string;
  }): OutboxEventEntity {
    const eventId = randomUUID();
    const envelope = {
      eventId,
      eventType: params.eventType,
      version: 1,
      correlationId: params.correlationId,
      causationId: params.causationId,
      sagaId: params.correlationId,
      requestId: params.requestId,
      timestamp: new Date().toISOString(),
      payload: params.payload,
    };

    const activeTraceHeaders: Record<string, string> = {};
    try {
      propagation.inject(context.active(), activeTraceHeaders);
    } catch {
      // Safe fallback
    }

    return new OutboxEventEntity(
      eventId,
      params.aggregateId,
      params.aggregateType,
      params.eventType,
      envelope,
      OutboxStatus.PENDING,
      params.requestId,
      params.correlationId,
      params.causationId,
      new Date(),
      null,
      0,
      activeTraceHeaders
    );
  }
}
