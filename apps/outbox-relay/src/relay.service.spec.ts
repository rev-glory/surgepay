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
      markPublished: jest.fn(),
      markFailed: jest.fn(),
      markRetrying: jest.fn(),
      countPending: jest.fn().mockResolvedValue(1),
      countFailed: jest.fn().mockResolvedValue(2),
      countPublished: jest.fn().mockResolvedValue(3),
    } as unknown as jest.Mocked<OutboxRepository>;

    publisher = {
      publish: jest.fn(),
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
      },
    } as unknown as jest.Mocked<ConfigService>;

    metricsService = {
      setOutboxPending: jest.fn(),
      setOutboxPublished: jest.fn(),
      setOutboxFailed: jest.fn(),
      recordOutboxLag: jest.fn(),
      recordPublicationRetry: jest.fn(),
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

  const mockEvent = (status: OutboxStatus = OutboxStatus.PUBLISHING, retryCount = 0): OutboxEvent => ({
    id: 'e1d6d538-4e89-49ea-994c-47ea55b57f0d',
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
  });

  it('empty batches run stale recovery but perform no publisher delegation', async () => {
    poller.pollPending.mockResolvedValue([]);

    await relayService.processBatch();

    expect(poller.recoverStale).toHaveBeenCalledWith(300000, 3);
    expect(poller.pollPending).toHaveBeenCalledWith(100);
    expect(publisher.publish).not.toHaveBeenCalled();

    // Verify gauges are synced
    expect(metricsService.setOutboxPending).toHaveBeenCalledWith('outbox-relay', 1);
    expect(metricsService.setOutboxFailed).toHaveBeenCalledWith('outbox-relay', 2);
    expect(metricsService.setOutboxPublished).toHaveBeenCalledWith('outbox-relay', 3);
  });

  it('delegates to publisher and transitions Outbox status atomically to PUBLISHED', async () => {
    const event = mockEvent(OutboxStatus.PUBLISHING);
    poller.pollPending.mockResolvedValue([event]);
    publisher.publish.mockResolvedValue({ partition: 2, offset: '1024' });
    repository.markPublished.mockResolvedValue({ ...event, status: OutboxStatus.PUBLISHED, partition: 2, offset: '1024' });

    await relayService.processBatch();

    expect(poller.recoverStale).toHaveBeenCalledWith(300000, 3);
    expect(poller.pollPending).toHaveBeenCalledWith(100);
    expect(publisher.publish).toHaveBeenCalledWith(event);
    expect(repository.markPublished).toHaveBeenCalledWith(event.id, 2, '1024');

    // Verify lag metric recorded
    expect(metricsService.recordOutboxLag).toHaveBeenCalledWith('outbox-relay', 'PaymentInitiated', expect.any(Number));
  });

  it('publisher boundary failures increment retry count and transition retryable events to RETRYING', async () => {
    const event = mockEvent(OutboxStatus.PUBLISHING, 0);
    poller.pollPending.mockResolvedValue([event]);
    const publishError = new Error('Publish timeout or network failure');
    publisher.publish.mockRejectedValue(publishError);
    repository.markFailed.mockResolvedValue({ ...event, status: OutboxStatus.FAILED, retryCount: 1 });
    repository.markRetrying.mockResolvedValue({ ...event, status: OutboxStatus.RETRYING, retryCount: 1 });

    await expect(relayService.processBatch()).rejects.toThrow('Publish timeout or network failure');

    expect(publisher.publish).toHaveBeenCalledWith(event);
    expect(repository.markFailed).toHaveBeenCalledWith(event.id, 'Publish timeout or network failure');
    expect(repository.markRetrying).toHaveBeenCalledWith(event.id);

    // Verify retry counter incremented
    expect(metricsService.recordPublicationRetry).toHaveBeenCalledWith('outbox-relay', 'PaymentInitiated');
  });

  it('publisher boundary failures do not transition to RETRYING if retry limit is exhausted', async () => {
    const event = mockEvent(OutboxStatus.PUBLISHING, 2); // next attempt will be 3 (equals retryLimit)
    poller.pollPending.mockResolvedValue([event]);
    const publishError = new Error('Broker unavailable');
    publisher.publish.mockRejectedValue(publishError);
    repository.markFailed.mockResolvedValue({ ...event, status: OutboxStatus.FAILED, retryCount: 3 });

    await expect(relayService.processBatch()).rejects.toThrow('Broker unavailable');

    expect(publisher.publish).toHaveBeenCalledWith(event);
    expect(repository.markFailed).toHaveBeenCalledWith(event.id, 'Broker unavailable');
    expect(repository.markRetrying).not.toHaveBeenCalled();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('Outbox event retry limit exhausted. Left in FAILED state.'),
      expect.anything(),
    );
  });
});
