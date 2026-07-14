import { createHash, randomUUID } from 'crypto';
import { Injectable } from '@nestjs/common';
import { KafkaEventProducer, LoggerService } from '@surgepay/common';
import {
  BaseEventEnvelope,
  RETRY_EXHAUSTED,
  RETRY_SCHEDULED,
  SAGA_RETRY_REGISTERED,
  SAGA_STEP_EXECUTION_FAILED,
  type SagaRetryRegisteredEvent,
  type SagaStepExecutionFailedEvent,
} from '@surgepay/events';
import { ScheduledRetryEntity } from '../entities/scheduled-retry.entity';
import { RetryRepository } from '../repositories/retry.repository';

@Injectable()
export class RetrySchedulerService {
  constructor(
    private readonly retryRepository: RetryRepository,
    private readonly eventProducer: KafkaEventProducer,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('RetrySchedulerService');
  }

  async schedule(payload: {
    originalTopic: string;
    originalEvent: any;
    retryCount: number;
    maxAttempts: number;
    baseDelayMs: number;
    maxDelayMs: number;
  }): Promise<void> {
    const { originalTopic, originalEvent, retryCount, maxAttempts, baseDelayMs, maxDelayMs } = payload;
    const originalEventId = originalEvent.eventId;

    // Authoritative retry attempt calculation
    const nextAttempt = retryCount + 1;

    // Generate deterministic ScheduledRetry ID to enforce idempotency
    const deterministicId = createHash('sha256')
      .update(`${originalEventId}_${nextAttempt}`)
      .digest('hex');

    this.logger.info(`Scheduling retry attempt #${nextAttempt} for event ${originalEventId}`, {
      sagaId: originalEvent.sagaId,
      nextAttempt,
      maxAttempts,
    });

    if (nextAttempt > maxAttempts) {
      // Bounded retry limits exhausted: Permanent Failure Classified
      this.logger.warn(`Retry attempts exhausted for event ${originalEventId} (${nextAttempt}/${maxAttempts})`);

      // 1. Publish to DLQ
      const dlqPayload = {
        originalEvent,
        failureReason: `Retry attempts exhausted after ${maxAttempts} attempts`,
        retryCount: nextAttempt - 1,
        consumer: 'retry-scheduler',
        failedAt: new Date().toISOString(),
        dlqTopic: originalTopic,
      };

      const dlqEnvelope = {
        eventId: randomUUID(),
        eventType: 'DeadLetterRecord',
        correlationId: originalEvent.correlationId,
        causationId: originalEvent.eventId,
        sagaId: originalEvent.sagaId,
        timestamp: new Date().toISOString(),
        version: 1,
        payload: dlqPayload,
      };

      await this.eventProducer.publish('payments.dlq', originalEvent.sagaId || '', dlqEnvelope);

      // 2. Publish Saga-facing business failure event
      const failurePayload = {
        sagaId: originalEvent.sagaId || '',
        originalEventId,
        originalTopic,
        attempts: nextAttempt - 1,
        failureReason: `Retry attempts exhausted after ${maxAttempts} attempts`,
        failedAt: new Date().toISOString(),
      };

      const failureEnvelope: SagaStepExecutionFailedEvent = {
        eventId: randomUUID(),
        eventType: SAGA_STEP_EXECUTION_FAILED,
        correlationId: originalEvent.correlationId,
        causationId: originalEvent.eventId,
        sagaId: originalEvent.sagaId || '',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: failurePayload,
      };

      await this.eventProducer.publish('retry.events', originalEvent.sagaId || '', failureEnvelope);

      // 3. Publish operational telemetry RetryExhausted
      const operationalExhausted = {
        eventId: randomUUID(),
        eventType: RETRY_EXHAUSTED,
        correlationId: originalEvent.correlationId,
        causationId: originalEvent.eventId,
        sagaId: originalEvent.sagaId || '',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          originalEventId,
          topic: originalTopic,
          attempts: nextAttempt - 1,
          exhaustedAt: new Date().toISOString(),
        },
      };

      await this.eventProducer.publish('retry.events', originalEvent.sagaId || '', operationalExhausted);
      return;
    }

    // Calculate delay with exponential backoff & randomized proportional +/- 10% jitter
    const backoff = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, nextAttempt - 1));
    const jitteredDelay = Math.round(backoff * (0.9 + Math.random() * 0.2));
    const executeAt = new Date(Date.now() + jitteredDelay);

    const retryEntity = ScheduledRetryEntity.create({
      id: deterministicId,
      originalTopic,
      originalMessage: originalEvent,
      retryCount: nextAttempt,
      maxAttempts,
      baseDelayMs,
      maxDelayMs,
      correlationId: originalEvent.correlationId,
      causationId: originalEvent.eventId,
      sagaId: originalEvent.sagaId || null,
      executeAt,
    });

    try {
      await this.retryRepository.save(retryEntity);
    } catch (err: unknown) {
      const prismaError = err as { code?: string };
      if (prismaError.code === 'P2002') {
        this.logger.warn(`Idempotency hit: ScheduledRetry ${deterministicId} already scheduled. Skipping.`, {
          originalEventId,
          nextAttempt,
        });
        return;
      }
      throw err;
    }

    // Publish Saga-facing business success event
    const registeredPayload = {
      sagaId: originalEvent.sagaId || '',
      originalEventId,
      attempt: nextAttempt,
      nextExecutionTime: executeAt.toISOString(),
    };

    const registeredEnvelope: SagaRetryRegisteredEvent = {
      eventId: randomUUID(),
      eventType: SAGA_RETRY_REGISTERED,
      correlationId: originalEvent.correlationId,
      causationId: originalEvent.eventId,
      sagaId: originalEvent.sagaId || '',
      timestamp: new Date().toISOString(),
      version: 1,
      payload: registeredPayload,
    };

    await this.eventProducer.publish('retry.events', originalEvent.sagaId || '', registeredEnvelope);

    // Publish operational telemetry RetryScheduled
    const operationalScheduled = {
      eventId: randomUUID(),
      eventType: RETRY_SCHEDULED,
      correlationId: originalEvent.correlationId,
      causationId: originalEvent.eventId,
      sagaId: originalEvent.sagaId || '',
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        originalEventId,
        topic: originalTopic,
        attempt: nextAttempt,
        maxAttempts,
        nextExecutionTime: executeAt.toISOString(),
      },
    };

    await this.eventProducer.publish('retry.events', originalEvent.sagaId || '', operationalScheduled);

    this.logger.info(`Durable retry successfully scheduled for event ${originalEventId} at ${executeAt.toISOString()}`);
  }
}
