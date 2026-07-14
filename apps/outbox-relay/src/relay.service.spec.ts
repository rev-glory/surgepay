import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService, MetricsService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { type OutboxEvent, OutboxStatus } from './generated/client';
import { OutboxPoller } from './poller';
import { EVENT_PUBLISHER, type EventPublisher } from './publisher';
import { OutboxRelayService } from './relay.service';
import { OutboxRepository } from './repositories/outbox.repository';

describe('OutboxRelayService', () => {
  let relayService: OutboxRelayService;
  let poller: jest.Mocked<OutboxPoller>;
  let repository: jest.Mocked<OutboxRepository>;
  let publisher: jest.Mocked<EventPublisher>;
  let logger: jest.Mocked<LoggerService>;
  let config: jest.Mocked<ConfigService>;
  let metricsService: jest.Mocked<MetricsService>;

  beforeEach(async () => {
    poller = {
      pollPending: jest.fn(),
      recoverStale: jest.fn(),
    } as unknown as jest.Mocked<OutboxPoller>;

    repository = {
      markPublishedBatch: jest.fn(),
      markFailedBatch: jest.fn(),
      countPending: jest.fn().mockResolvedValue(1),
      countFailed: jest.fn().mockResolvedValue(2),
      countPublished: jest.fn().mockResolvedValue(3),
    } as unknown as jest.Mocked<OutboxRepository>;

    publisher = {
      publish: jest.fn(),
      publishBatch: jest.fn(),
    } as unknown as jest.Mocked<EventPublisher>;

    logger = {
      setContext: jest.fn(),
      debug: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    } as unknown as jest.Mocked<LoggerService>;

    config = {
      logging: {
        serviceName: 'outbox-relay',
      },
      outbox: {
        batchSize: 100,
        pollingInterval: 500,
        retryLimit: 3,
        publishTimeout: 5000,
        staleTimeoutMs: 300000,
        maxInFlightMessages: 1000,
        flushInterval: 100,
      },
    } as unknown as jest.Mocked<ConfigService>;

    metricsService = {
      setOutboxPending: jest.fn(),
      setOutboxPublished: jest.fn(),
      setOutboxFailed: jest.fn(),
      recordOutboxLag: jest.fn(),
      recordPublicationRetry: jest.fn(),
      setOutboxInFlight: jest.fn(),
      recordOutboxBatchSize: jest.fn(),
      recordOutboxCycleDuration: jest.fn(),
    } as unknown as jest.Mocked<MetricsService>;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OutboxRelayService,
        { provide: OutboxPoller, useValue: poller },
        { provide: OutboxRepository, useValue: repository },
        { provide: EVENT_PUBLISHER, useValue: publisher },
        { provide: ConfigService, useValue: config },
        { provide: LoggerService, useValue: logger },
        { provide: MetricsService, useValue: metricsService },
      ],
    }).compile();

    relayService = module.get<OutboxRelayService>(OutboxRelayService);
  });

  const mockEvent = (id: string, status: OutboxStatus = OutboxStatus.PUBLISHING, retryCount = 0): OutboxEvent => ({
    id,
    aggregateId: '0979bf3b-9a40-4258-8687-9bb3a62ea6f3',
    aggregateType: 'Payment',
    eventType: 'PaymentInitiated',
    payload: { amount: 5000 },
    status,
    requestId: 'req_1',
    correlationId: 'corr_1',
    causationId: 'caus_1',
    createdAt: new Date(),
    publishedAt: null,
    retryCount,
    partition: null,
    offset: null,
    lastAttemptAt: new Date(),
    traceHeaders: null,
  });

  it('empty batches run stale recovery but perform no publisher delegation', async () => {
    poller.pollPending.mockResolvedValue([]);

    await relayService.processBatch();

    expect(poller.recoverStale).toHaveBeenCalledWith(300000, 3);
    expect(poller.pollPending).toHaveBeenCalledWith(100);
    expect(publisher.publishBatch).not.toHaveBeenCalled();

    // Verify gauges are synced
    expect(metricsService.setOutboxPending).toHaveBeenCalledWith('outbox-relay', 1);
    expect(metricsService.setOutboxFailed).toHaveBeenCalledWith('outbox-relay', 2);
    expect(metricsService.setOutboxPublished).toHaveBeenCalledWith('outbox-relay', 3);
  });

  it('delegates to publisher and transitions Outbox status atomically to PUBLISHED (batch)', async () => {
    const event = mockEvent('event-1', OutboxStatus.PUBLISHING);
    poller.pollPending.mockResolvedValue([event]);
    publisher.publishBatch.mockResolvedValue([{ id: 'event-1', partition: 2, offset: '1024' }]);

    await relayService.processBatch();

    // Await background promise to execute
    await new Promise(resolve => setImmediate(resolve));

    expect(poller.recoverStale).toHaveBeenCalledWith(300000, 3);
    expect(poller.pollPending).toHaveBeenCalledWith(100);
    expect(publisher.publishBatch).toHaveBeenCalledWith([event]);
    expect(repository.markPublishedBatch).toHaveBeenCalledWith([{ id: 'event-1', partition: 2, offset: '1024' }]);

    // Verify lag metric recorded
    expect(metricsService.recordOutboxLag).toHaveBeenCalledWith('outbox-relay', 'PaymentInitiated', expect.any(Number));
  });

  it('publisher boundary failures trigger markFailedBatch update', async () => {
    const event = mockEvent('event-1', OutboxStatus.PUBLISHING, 0);
    poller.pollPending.mockResolvedValue([event]);
    const publishError = new Error('Publish timeout or network failure');
    publisher.publishBatch.mockRejectedValue(publishError);

    await expect(relayService.processBatch()).rejects.toThrow(publishError);

    expect(publisher.publishBatch).toHaveBeenCalledWith([event]);
    expect(repository.markFailedBatch).toHaveBeenCalledWith(['event-1'], 'Publish timeout or network failure', 3);

    // Verify retry counter incremented
    expect(metricsService.recordPublicationRetry).toHaveBeenCalledWith('outbox-relay', 'PaymentInitiated');
  });

  it('respects maxInFlightMessages back-pressure cap', async () => {
    // Set max in-flight to 1, and simulate 1 active in-flight batch
    relayService['activeInFlight'] = 1;
    config.outbox.maxInFlightMessages = 1;

    await relayService.processBatch();

    // Verify it did not poll since capacity was 0
    expect(poller.pollPending).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('Relay capacity saturated. Skipping poll to apply back-pressure.'),
      expect.any(Object)
    );
  });
});
