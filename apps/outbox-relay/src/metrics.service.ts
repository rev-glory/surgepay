import { Inject, Injectable } from '@nestjs/common';
import { LoggerService } from '@surgepay/common';

@Injectable()
export class RelayMetrics {
  constructor(@Inject(LoggerService) private readonly logger: LoggerService) {
    this.logger.setContext('RelayMetrics');
  }

  /**
   * Records the duration and details of a single polling cycle.
   */
  recordPollCycle(durationMs: number, discoveredCount: number): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }

  /**
   * Records a successful event publication and its latency.
   */
  recordPublishSuccess(eventType: string, latencyMs?: number): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }

  /**
   * Records a failed event publication, tracking retries and transient flags.
   */
  recordPublishFailure(eventType: string, isTransient: boolean, retryCount?: number): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }

  /**
   * Records the latency (lag) between outbox entry insertion and publish completion.
   */
  recordOutboxLag(createdAt: Date): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }

  /**
   * Records the current total of pending events awaiting execution.
   */
  recordPendingCount(count: number): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }

  /**
   * Records the current total of permanently failed events.
   */
  recordFailedCount(count: number): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }
}
