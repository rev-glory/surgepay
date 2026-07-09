import { Test, TestingModule } from '@nestjs/testing';
import { LoggerService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { RelayService } from './relay.service';
import { Poller } from './poller';
import { OutboxPublisher } from './publisher';
import { PrismaService } from './prisma.service';
import { RelayMetrics } from './metrics.service';

describe('RelayService', () => {
  let relayService: RelayService;
  let poller: jest.Mocked<Poller>;
  let publisher: jest.Mocked<OutboxPublisher>;
  let prismaService: any;

  beforeEach(async () => {
    const mockPoller = {
      pollPending: jest.fn(),
    };
    const mockPublisher = {
      publish: jest.fn(),
    };
    const mockPrismaService = {
      client: {
        $transaction: jest.fn(),
        outboxEvent: {
          updateMany: jest.fn(),
          update: jest.fn(),
        },
      } as any,
    };
    mockPrismaService.client.$transaction.mockImplementation((callback: any) =>
      callback(mockPrismaService.client),
    );
    const mockConfigService = {
      outbox: {
        batchSize: 100,
        publishTimeout: 5000,
      },
    };
    const mockLogger = {
      setContext: jest.fn(),
      info: jest.fn(),
      debug: jest.fn(),
      error: jest.fn(),
      warn: jest.fn(),
    };
    const mockMetrics = {
      recordPollCycle: jest.fn(),
      recordPublishSuccess: jest.fn(),
      recordPublishFailure: jest.fn(),
      recordOutboxLag: jest.fn(),
      recordPendingCount: jest.fn(),
      recordFailedCount: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RelayService,
        { provide: Poller, useValue: mockPoller },
        { provide: OutboxPublisher, useValue: mockPublisher },
        { provide: PrismaService, useValue: mockPrismaService },
        { provide: ConfigService, useValue: mockConfigService },
        { provide: LoggerService, useValue: mockLogger },
        { provide: RelayMetrics, useValue: mockMetrics },
      ],
    }).compile();

    relayService = module.get<RelayService>(RelayService);
    poller = module.get(Poller) as any;
    publisher = module.get(OutboxPublisher) as any;
    prismaService = module.get(PrismaService);
  });

  it('should process events when pending items exist', async () => {
    const mockEvent = {
      id: 'event-uuid',
      aggregateId: 'agg-uuid',
      aggregateType: 'payment',
      eventType: 'PaymentInitiated',
      payload: {},
      status: 'PENDING' as const,
      requestId: 'req-id',
      correlationId: 'corr-id',
      causationId: 'caus-id',
      createdAt: new Date(),
      publishedAt: null,
      retryCount: 0,
      topic: null,
      partition: null,
      offset: null,
      lastError: null,
      lastAttemptAt: null,
    };

    poller.pollPending.mockResolvedValue([mockEvent]);
    publisher.publish.mockResolvedValue([
      {
        topicName: 'payments.initiated',
        partition: 0,
        offset: '42',
        errorCode: 0,
      },
    ]);

    await relayService.runOnce();

    expect(poller.pollPending).toHaveBeenCalled();
    expect(publisher.publish).toHaveBeenCalledWith(mockEvent);
    expect(prismaService.client.outboxEvent.updateMany).toHaveBeenCalled();
    expect(prismaService.client.outboxEvent.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: mockEvent.id },
        data: expect.objectContaining({
          status: 'PUBLISHED',
          topic: 'payments.initiated',
          partition: 0,
          offset: '42',
        }),
      }),
    );
  });

  it('should skip processing when no events are found', async () => {
    poller.pollPending.mockResolvedValue([]);

    await relayService.runOnce();

    expect(poller.pollPending).toHaveBeenCalled();
    expect(publisher.publish).not.toHaveBeenCalled();
  });
});
