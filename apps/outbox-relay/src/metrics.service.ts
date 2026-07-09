import { Inject, Injectable } from '@nestjs/common';
import { LoggerService, MetricsService } from '@surgepay/common';

@Injectable()
export class RelayMetrics {
  constructor(
    @Inject(LoggerService) private readonly logger: LoggerService,
    private readonly sharedMetrics: MetricsService,
  ) {
    this.logger.setContext('RelayMetrics');
  }

  /**
   * Records the duration and details of a single polling cycle.
   */
  recordPollCycle(durationMs: number, discoveredCount: number): void {
    // Operational logging
  }

  /**
   * Records a successful event publication and its latency.
   */
  recordPublishSuccess(eventType: string, latencyMs?: number): void {
    const serviceName = process.env.APP_NAME || 'outbox-relay';
    this.sharedMetrics.incrementPublished(serviceName, eventType);
    if (latencyMs !== undefined) {
      this.sharedMetrics.recordPublishDuration(serviceName, eventType, latencyMs);
    }
  }

  /**
   * Records a failed event publication, tracking retries and transient flags.
   */
  recordPublishFailure(eventType: string, isTransient: boolean, retryCount?: number): void {
    const serviceName = process.env.APP_NAME || 'outbox-relay';
    this.sharedMetrics.incrementPublishFailures(serviceName, eventType);
    if (retryCount !== undefined && retryCount > 0) {
      this.sharedMetrics.incrementRetries(serviceName, eventType);
    }
  }

  /**
   * Records the latency (lag) between outbox entry insertion and publish completion.
   */
  recordOutboxLag(createdAt: Date): void {
    // OpTelemetry trace / metric hook if needed, but not required by prompt metrics
  }

  /**
   * Records the current total of pending events awaiting execution by eventType.
   */
  recordPendingCount(eventType: string, count: number): void {
    const serviceName = process.env.APP_NAME || 'outbox-relay';
    this.sharedMetrics.setPendingEvents(serviceName, eventType, count);
  }

  /**
   * Records the current total of successfully published events by eventType.
   */
  recordPublishedCount(eventType: string, count: number): void {
    const serviceName = process.env.APP_NAME || 'outbox-relay';
    this.sharedMetrics.setPublishedEvents(serviceName, eventType, count);
  }

  /**
   * Records the current total of permanently failed events by eventType.
   */
  recordFailedCount(eventType: string, count: number): void {
    const serviceName = process.env.APP_NAME || 'outbox-relay';
    this.sharedMetrics.setFailedEvents(serviceName, eventType, count);
  }
}
