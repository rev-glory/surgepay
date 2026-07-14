import { RetryStatus } from '../generated/client';

export class ScheduledRetryEntity {
  constructor(
    public readonly id: string,
    public readonly originalTopic: string,
    public readonly originalMessage: Record<string, any>,
    public readonly retryCount: number,
    public readonly maxAttempts: number,
    public readonly baseDelayMs: number,
    public readonly maxDelayMs: number,
    public readonly correlationId: string,
    public readonly causationId: string,
    public readonly sagaId: string | null,
    public readonly executeAt: Date,
    public status: RetryStatus,
    public readonly createdAt: Date,
    public readonly updatedAt: Date
  ) {}

  static create(params: {
    id: string;
    originalTopic: string;
    originalMessage: Record<string, any>;
    retryCount: number;
    maxAttempts: number;
    baseDelayMs: number;
    maxDelayMs: number;
    correlationId: string;
    causationId: string;
    sagaId: string | null;
    executeAt: Date;
  }): ScheduledRetryEntity {
    return new ScheduledRetryEntity(
      params.id,
      params.originalTopic,
      params.originalMessage,
      params.retryCount,
      params.maxAttempts,
      params.baseDelayMs,
      params.maxDelayMs,
      params.correlationId,
      params.causationId,
      params.sagaId,
      params.executeAt,
      RetryStatus.PENDING,
      new Date(),
      new Date()
    );
  }
}
