import { Inject, Injectable, Optional } from '@nestjs/common';
import { Counter, Gauge, Histogram,Registry } from 'prom-client';

export { Registry } from 'prom-client';

export const processRegistry = new Registry();

@Injectable()
export class MetricsService {
  public readonly registry: Registry;

  // Producer/Publication Metrics
  private readonly eventsPublished: Counter<string>;
  private readonly publishFailures: Counter<string>;
  private readonly publishDuration: Histogram<string>;
  private readonly retryTotal: Counter<string>;

  // Consumer Metrics
  private readonly eventsConsumed: Counter<string>;
  private readonly duplicateEvents: Counter<string>;
  private readonly consumerFailures: Counter<string>;
  private readonly consumerDuration: Histogram<string>;

  // Outbox Metrics
  private readonly outboxPending: Gauge<string>;
  private readonly outboxPublished: Gauge<string>;
  private readonly outboxFailed: Gauge<string>;
  private readonly outboxLag: Histogram<string>;

  // Inbox & DLQ Metrics
  private readonly inboxReceived: Counter<string>;
  private readonly inboxProcessed: Counter<string>;
  private readonly inboxDlqEvents: Counter<string>;
  private readonly inboxDlqDepth: Gauge<string>;

  constructor(
    @Optional() @Inject('CUSTOM_REGISTRY') customRegistry?: Registry,
  ) {
    this.registry = customRegistry || processRegistry;

    // Helper function to register or retrieve metrics dynamically
    const getOrRegisterCounter = <T extends string>(name: string, help: string, labelNames: T[]): Counter<T> => {
      const existing = this.registry.getSingleMetric(name);
      if (existing) {
        return existing as Counter<T>;
      }
      return new Counter({
        name,
        help,
        labelNames,
        registers: [this.registry],
      });
    };

    const getOrRegisterGauge = <T extends string>(name: string, help: string, labelNames: T[]): Gauge<T> => {
      const existing = this.registry.getSingleMetric(name);
      if (existing) {
        return existing as Gauge<T>;
      }
      return new Gauge({
        name,
        help,
        labelNames,
        registers: [this.registry],
      });
    };

    const getOrRegisterHistogram = <T extends string>(
      name: string,
      help: string,
      labelNames: T[],
      buckets: number[],
    ): Histogram<T> => {
      const existing = this.registry.getSingleMetric(name);
      if (existing) {
        return existing as Histogram<T>;
      }
      return new Histogram({
        name,
        help,
        labelNames,
        buckets,
        registers: [this.registry],
      });
    };

    // 1. Producer / Publication Metrics
    this.eventsPublished = getOrRegisterCounter('events_published_total', 'Count of successfully acknowledged Kafka events', ['service', 'eventType', 'status']);
    this.publishFailures = getOrRegisterCounter('publish_failures_total', 'Count of failed Kafka publication attempts', ['service', 'eventType', 'status']);
    this.publishDuration = getOrRegisterHistogram(
      'publish_duration_ms',
      'Kafka publication latency in ms',
      ['service', 'eventType', 'status'],
      [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
    );
    this.retryTotal = getOrRegisterCounter('retry_total', 'Cumulative count of application-visible event publication retry attempts', ['service', 'eventType', 'status']);

    // 2. Consumer Metrics
    this.eventsConsumed = getOrRegisterCounter('events_consumed_total', 'Total events received by the consumer (including duplicates)', ['service', 'eventType', 'consumer', 'status']);
    this.duplicateEvents = getOrRegisterCounter('duplicate_events_total', 'Events identified as duplicates and skipped', ['service', 'eventType', 'consumer', 'status']);
    this.consumerFailures = getOrRegisterCounter('consumer_failures_total', 'Cumulative handler execution failures', ['service', 'eventType', 'consumer', 'status']);
    this.consumerDuration = getOrRegisterHistogram(
      'consumer_duration_ms',
      'Business handler execution duration in ms',
      ['service', 'eventType', 'consumer', 'status'],
      [10, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
    );

    // 3. Outbox Metrics
    this.outboxPending = getOrRegisterGauge('outbox_pending_events', 'Count of outbox records awaiting publication', ['service']);
    this.outboxPublished = getOrRegisterGauge('outbox_published_events', 'Count of outbox records in PUBLISHED state', ['service']);
    this.outboxFailed = getOrRegisterGauge('outbox_failed_events', 'Count of outbox records in permanent FAILED state', ['service']);
    this.outboxLag = getOrRegisterHistogram(
      'outbox_lag_ms',
      'Latency between outbox event database creation and successful Kafka publish',
      ['service', 'eventType'],
      [100, 500, 1000, 2500, 5000, 10000, 30000, 60000],
    );

    // 4. Inbox & DLQ Metrics
    this.inboxReceived = getOrRegisterCounter('inbox_received_events_total', 'Cumulative events recorded as RECEIVED', ['service', 'consumer', 'eventType']);
    this.inboxProcessed = getOrRegisterCounter('inbox_processed_events_total', 'Cumulative events marked PROCESSED', ['service', 'consumer', 'eventType']);
    this.inboxDlqEvents = getOrRegisterCounter('inbox_dlq_events_total', 'Cumulative events sent to DLQ', ['service', 'consumer', 'eventType']);
    this.inboxDlqDepth = getOrRegisterGauge('inbox_dlq_depth', 'Number of unresolved dead-lettered events in the local database', ['service', 'consumer']);
  }

  // --- Helper Methods for Producer ---
  recordPublishSuccess(service: string, eventType: string, durationMs: number): void {
    this.eventsPublished.inc({ service, eventType, status: 'success' });
    this.publishDuration.observe({ service, eventType, status: 'success' }, durationMs);
  }

  recordPublishFailure(service: string, eventType: string, durationMs: number): void {
    this.publishFailures.inc({ service, eventType, status: 'failure' });
    this.publishDuration.observe({ service, eventType, status: 'failure' }, durationMs);
  }

  recordPublicationRetry(service: string, eventType: string): void {
    this.retryTotal.inc({ service, eventType, status: 'retry' });
  }

  // --- Helper Methods for Consumer ---
  recordConsumeAttempt(service: string, eventType: string, consumer: string): void {
    this.eventsConsumed.inc({ service, eventType, consumer, status: 'received' });
  }

  recordDuplicateSkip(service: string, eventType: string, consumer: string): void {
    this.duplicateEvents.inc({ service, eventType, consumer, status: 'skipped' });
  }

  recordHandlerFailure(service: string, eventType: string, consumer: string): void {
    this.consumerFailures.inc({ service, eventType, consumer, status: 'failure' });
  }

  recordHandlerDuration(service: string, eventType: string, consumer: string, status: 'success' | 'failure', durationMs: number): void {
    this.consumerDuration.observe({ service, eventType, consumer, status }, durationMs);
  }

  // --- Helper Methods for Outbox ---
  setOutboxPending(service: string, count: number): void {
    this.outboxPending.set({ service }, count);
  }

  setOutboxPublished(service: string, count: number): void {
    this.outboxPublished.set({ service }, count);
  }

  setOutboxFailed(service: string, count: number): void {
    this.outboxFailed.set({ service }, count);
  }

  recordOutboxLag(service: string, eventType: string, lagMs: number): void {
    this.outboxLag.observe({ service, eventType }, lagMs);
  }

  // --- Helper Methods for Inbox & DLQ ---
  recordInboxReceived(service: string, consumer: string, eventType: string): void {
    this.inboxReceived.inc({ service, consumer, eventType });
  }

  recordInboxProcessed(service: string, consumer: string, eventType: string): void {
    this.inboxProcessed.inc({ service, consumer, eventType });
  }

  recordInboxDlqEvent(service: string, consumer: string, eventType: string): void {
    this.inboxDlqEvents.inc({ service, consumer, eventType });
  }

  setInboxDlqDepth(service: string, consumer: string, depth: number): void {
    this.inboxDlqDepth.set({ service, consumer }, depth);
  }
}
