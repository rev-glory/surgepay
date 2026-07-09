import { Injectable } from '@nestjs/common';
import { Counter, Gauge, Histogram, register } from 'prom-client';

import { CONSUMER_METRICS } from './consumer.metrics';
import { INBOX_METRICS } from './inbox.metrics';
import { OUTBOX_METRICS } from './outbox.metrics';
import { PRODUCER_METRICS } from './producer.metrics';

@Injectable()
export class MetricsService {
  private readonly publishedTotal: Counter;
  private readonly publishDuration: Histogram;
  private readonly publishFailures: Counter;
  private readonly retryTotal: Counter;

  private readonly consumedTotal: Counter;
  private readonly consumerDuration: Histogram;
  private readonly duplicateTotal: Counter;
  private readonly consumerFailures: Counter;

  private readonly pendingEvents: Gauge;
  private readonly publishedEvents: Gauge;
  private readonly failedEvents: Gauge;

  private readonly relayBatchSize: Histogram;
  private readonly relayPublishDuration: Histogram;
  private readonly relayInFlight: Gauge;

  private readonly receivedEvents: Counter;
  private readonly processedEvents: Counter;
  private readonly dlqEvents: Counter;

  constructor() {
    // Producer Metrics
    this.publishedTotal = this.getOrCreateCounter(
      PRODUCER_METRICS.PUBLISHED_TOTAL,
      'Total number of events published to Kafka',
      ['service', 'eventType'],
    );
    this.publishDuration = this.getOrCreateHistogram(
      PRODUCER_METRICS.DURATION_MS,
      'Latency of event publishing to Kafka in milliseconds',
      ['service', 'eventType'],
    );
    this.publishFailures = this.getOrCreateCounter(
      PRODUCER_METRICS.FAILURES_TOTAL,
      'Total number of event publishing failures',
      ['service', 'eventType'],
    );
    this.retryTotal = this.getOrCreateCounter(
      PRODUCER_METRICS.RETRY_TOTAL,
      'Total number of publishing retry attempts',
      ['service', 'eventType'],
    );

    // Consumer Metrics
    this.consumedTotal = this.getOrCreateCounter(
      CONSUMER_METRICS.CONSUMED_TOTAL,
      'Total number of events consumed',
      ['service', 'eventType', 'consumer'],
    );
    this.consumerDuration = this.getOrCreateHistogram(
      CONSUMER_METRICS.DURATION_MS,
      'Latency of event consumption in milliseconds',
      ['service', 'eventType', 'consumer'],
    );
    this.duplicateTotal = this.getOrCreateCounter(
      CONSUMER_METRICS.DUPLICATES_TOTAL,
      'Total number of duplicate events suppressed',
      ['service', 'eventType', 'consumer'],
    );
    this.consumerFailures = this.getOrCreateCounter(
      CONSUMER_METRICS.FAILURES_TOTAL,
      'Total number of event consumption failures',
      ['service', 'eventType', 'consumer'],
    );

    // Outbox Metrics
    this.pendingEvents = this.getOrCreateGauge(
      OUTBOX_METRICS.PENDING,
      'Current number of pending outbox events',
      ['service', 'eventType'],
    );
    this.publishedEvents = this.getOrCreateGauge(
      OUTBOX_METRICS.PUBLISHED,
      'Current number of successfully published outbox events',
      ['service', 'eventType'],
    );
    this.failedEvents = this.getOrCreateGauge(
      OUTBOX_METRICS.FAILED,
      'Current number of permanently failed outbox events',
      ['service', 'eventType'],
    );

    this.relayBatchSize = this.getOrCreateHistogram(
      OUTBOX_METRICS.BATCH_SIZE,
      'Distribution of Outbox Relay batch sizes',
      ['service'],
    );
    this.relayPublishDuration = this.getOrCreateHistogram(
      OUTBOX_METRICS.PUBLISH_DURATION,
      'Publish duration for Outbox Relay batches in ms',
      ['service'],
    );
    this.relayInFlight = this.getOrCreateGauge(
      OUTBOX_METRICS.IN_FLIGHT,
      'Current number of in-flight messages in Outbox Relay',
      ['service'],
    );

    // Inbox Metrics
    this.receivedEvents = this.getOrCreateCounter(
      INBOX_METRICS.RECEIVED,
      'Total number of events persisted in the Inbox',
      ['service', 'eventType', 'consumer'],
    );
    this.processedEvents = this.getOrCreateCounter(
      INBOX_METRICS.PROCESSED,
      'Total number of events successfully processed in the Inbox',
      ['service', 'eventType', 'consumer'],
    );
    this.dlqEvents = this.getOrCreateCounter(
      INBOX_METRICS.DLQ,
      'Total number of events routed to the Dead Letter Queue',
      ['service', 'eventType', 'consumer'],
    );
  }

  private getOrCreateCounter(name: string, help: string, labelNames: string[]): Counter {
    const existing = register.getSingleMetric(name);
    if (existing) return existing as Counter;
    return new Counter({ name, help, labelNames });
  }

  private getOrCreateGauge(name: string, help: string, labelNames: string[]): Gauge {
    const existing = register.getSingleMetric(name);
    if (existing) return existing as Gauge;
    return new Gauge({ name, help, labelNames });
  }

  private getOrCreateHistogram(name: string, help: string, labelNames: string[]): Histogram {
    const existing = register.getSingleMetric(name);
    if (existing) return existing as Histogram;
    return new Histogram({ name, help, labelNames, buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000] });
  }

  // Producer instrumentation
  incrementPublished(service: string, eventType: string): void {
    this.publishedTotal.inc({ service, eventType });
  }

  recordPublishDuration(service: string, eventType: string, durationMs: number): void {
    this.publishDuration.observe({ service, eventType }, durationMs);
  }

  incrementPublishFailures(service: string, eventType: string): void {
    this.publishFailures.inc({ service, eventType });
  }

  incrementRetries(service: string, eventType: string): void {
    this.retryTotal.inc({ service, eventType });
  }

  // Consumer instrumentation
  incrementConsumed(service: string, eventType: string, consumer: string): void {
    this.consumedTotal.inc({ service, eventType, consumer });
  }

  recordConsumerDuration(service: string, eventType: string, consumer: string, durationMs: number): void {
    this.consumerDuration.observe({ service, eventType, consumer }, durationMs);
  }

  incrementDuplicates(service: string, eventType: string, consumer: string): void {
    this.duplicateTotal.inc({ service, eventType, consumer });
  }

  incrementConsumerFailures(service: string, eventType: string, consumer: string): void {
    this.consumerFailures.inc({ service, eventType, consumer });
  }

  // Outbox instrumentation
  setPendingEvents(service: string, eventType: string, count: number): void {
    this.pendingEvents.set({ service, eventType }, count);
  }

  setPublishedEvents(service: string, eventType: string, count: number): void {
    this.publishedEvents.set({ service, eventType }, count);
  }

  setFailedEvents(service: string, eventType: string, count: number): void {
    this.failedEvents.set({ service, eventType }, count);
  }

  recordRelayBatchSize(service: string, size: number): void {
    this.relayBatchSize.observe({ service }, size);
  }

  recordRelayPublishDuration(service: string, durationMs: number): void {
    this.relayPublishDuration.observe({ service }, durationMs);
  }

  setRelayInFlight(service: string, count: number): void {
    this.relayInFlight.set({ service }, count);
  }

  // Inbox instrumentation
  incrementReceived(service: string, eventType: string, consumer: string): void {
    this.receivedEvents.inc({ service, eventType, consumer });
  }

  incrementProcessed(service: string, eventType: string, consumer: string): void {
    this.processedEvents.inc({ service, eventType, consumer });
  }

  incrementDlqEvents(service: string, eventType: string, consumer: string): void {
    this.dlqEvents.inc({ service, eventType, consumer });
  }
}
