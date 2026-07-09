import { Injectable } from '@nestjs/common';
import { LoggerService } from '@surgepay/common';

@Injectable()
export class RelayMetrics {
  constructor(private readonly logger: LoggerService) {
    this.logger.setContext('RelayMetrics');
  }

  /**
   * Records the duration and details of a single polling cycle.
   */
  recordPollCycle(durationMs: number, discoveredCount: number): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }

  /**
   * Records a successful event publication.
   */
  recordPublishSuccess(eventType: string): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }

  /**
   * Records a failed event publication.
   */
  recordPublishFailure(eventType: string, isTransient: boolean): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }

  /**
   * Records the latency (lag) between outbox entry insertion and publish completion.
   */
  recordOutboxLag(createdAt: Date): void {
    // Instrumentation hook placeholder for future Prometheus integration
  }
}
