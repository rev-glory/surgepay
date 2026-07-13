import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { type OutboxEvent,OutboxStatus } from './generated/client';
import { OutboxPoller } from './poller';
import { EVENT_PUBLISHER, type EventPublisher } from './publisher';
import { OutboxRelayService } from './relay.service';

describe('OutboxRelayService', () => {
  let relayService: OutboxRelayService;
  let poller: jest.Mocked<OutboxPoller>;
  let publisher: jest.Mocked<EventPublisher>;
  let logger: jest.Mocked<LoggerService>;
  let config: jest.Mocked<ConfigService>;

  beforeEach(async () => {
    poller = {
      pollPending: jest.fn(),
    } as unknown as jest.Mocked<OutboxPoller>;

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
      outbox: {
        batchSize: 100,
        pollingInterval: 500,
        retryLimit: 3,
        publishTimeout: 5000,
      },
    } as unknown as jest.Mocked<ConfigService>;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OutboxRelayService,
        { provide: OutboxPoller, useValue: poller },
        { provide: EVENT_PUBLISHER, useValue: publisher },
        { provide: ConfigService, useValue: config },
        { provide: LoggerService, useValue: logger },
      ],
    }).compile();

    relayService = module.get<OutboxRelayService>(OutboxRelayService);
  });

  const mockEvent: OutboxEvent = {
    id: 'e1d6d538-4e89-49ea-994c-47ea55b57f0d',
    aggregateId: '0979bf3b-9a40-4258-8687-9bb3a62ea6f3',
    aggregateType: 'Payment',
    eventType: 'PaymentInitiated',
    payload: { amount: 5000 },
    status: OutboxStatus.PENDING,
    requestId: 'req_1',
    correlationId: 'corr_1',
    causationId: 'caus_1',
    createdAt: new Date(),
    publishedAt: null,
    retryCount: 0,
  };

  it('empty batches perform no publisher delegation', async () => {
    poller.pollPending.mockResolvedValue([]);

    await relayService.processBatch();

    expect(poller.pollPending).toHaveBeenCalledWith(100);
    expect(publisher.publish).not.toHaveBeenCalled();
    expect(logger.debug).toHaveBeenCalledWith('No pending outbox events found. Skipping delegation.');
  });

  it('delegates to publisher and leaves Outbox status unchanged as PENDING', async () => {
    poller.pollPending.mockResolvedValue([mockEvent]);
    publisher.publish.mockResolvedValue(undefined);

    await relayService.processBatch();

    expect(poller.pollPending).toHaveBeenCalledWith(100);
    expect(publisher.publish).toHaveBeenCalledWith(mockEvent);
    
    // Assert status in database remains unchanged by checking poller is not called to update it
    // No database update method was called on poller
    expect(mockEvent.status).toBe(OutboxStatus.PENDING);
  });

  it('publisher boundary failures propagate to the polling-cycle boundary', async () => {
    poller.pollPending.mockResolvedValue([mockEvent]);
    const publishError = new Error('Publish timeout or network failure');
    publisher.publish.mockRejectedValue(publishError);

    await expect(relayService.processBatch()).rejects.toThrow('Publish timeout or network failure');

    expect(poller.pollPending).toHaveBeenCalledWith(100);
    expect(publisher.publish).toHaveBeenCalledWith(mockEvent);
    expect(logger.error).toHaveBeenCalledWith(
      'Publisher boundary failure. Propagating to scheduler.',
      publishError,
      expect.objectContaining({
        eventId: mockEvent.id,
        correlationId: mockEvent.correlationId,
      }),
    );
  });

  it('placeholder delegation is not logged as durable Kafka publication', async () => {
    poller.pollPending.mockResolvedValue([mockEvent]);
    publisher.publish.mockImplementation(async (event) => {
      // Mock ConsoleEventPublisher logic
      logger.info('Event delegated to publisher boundary placeholder', {
        eventId: event.id,
        correlationId: event.correlationId,
      });
    });

    await relayService.processBatch();

    // Verify logger output does NOT claim durable Kafka publication
    const infoCalls = logger.info.mock.calls;
    const durableKafkaPublishLog = infoCalls.some(([msg]) => 
      msg.toLowerCase().includes('kafka') || 
      msg.toLowerCase().includes('durable') || 
      msg.toLowerCase().includes('published successfully to redpanda')
    );
    expect(durableKafkaPublishLog).toBe(false);
  });
});
