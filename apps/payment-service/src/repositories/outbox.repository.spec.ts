import type { LoggerService } from '@surgepay/common';

import { OutboxEventEntity } from '../entities/outbox-event.entity';
import { OutboxStatus } from '../generated/client';
import type { PrismaService } from '../prisma/prisma.service';
import { OutboxRepository } from './outbox.repository';

describe('OutboxRepository', () => {
  let repository: OutboxRepository;
  let prismaMock: {
    client: {
      outboxEvent: {
        create: jest.Mock;
        findMany: jest.Mock;
        findUnique: jest.Mock;
        update: jest.Mock;
      };
    };
  };
  let loggerMock: jest.Mocked<LoggerService>;

  beforeEach(() => {
    prismaMock = {
      client: {
        outboxEvent: {
          create: jest.fn(),
          findMany: jest.fn(),
          findUnique: jest.fn(),
          update: jest.fn(),
        },
      },
    };

    loggerMock = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    } as unknown as jest.Mocked<LoggerService>;

    repository = new OutboxRepository(prismaMock as unknown as PrismaService, loggerMock);
  });

  describe('save', () => {
    it('should create and return a new OutboxEvent record', async () => {
      const entity = OutboxEventEntity.create({
        aggregateId: '79999999-9999-4999-a999-999999999999',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: { eventId: 'event-1', correlationId: 'corr-1' },
        requestId: 'req-1',
        correlationId: 'corr-1',
        causationId: 'cause-1',
      });

      const dbModel = {
        id: entity.id,
        aggregateId: entity.aggregateId,
        aggregateType: entity.aggregateType,
        eventType: entity.eventType,
        payload: entity.payload,
        status: OutboxStatus.PENDING,
        requestId: 'req-1',
        correlationId: 'corr-1',
        causationId: 'cause-1',
        createdAt: entity.createdAt,
        publishedAt: null,
        retryCount: 0,
        traceHeaders: {},
      };

      prismaMock.client.outboxEvent.create.mockResolvedValue(dbModel);

      const result = await repository.save(entity);

      expect(prismaMock.client.outboxEvent.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          id: entity.id,
          aggregateId: entity.aggregateId,
          aggregateType: entity.aggregateType,
          eventType: entity.eventType,
          status: OutboxStatus.PENDING,
          requestId: 'req-1',
          correlationId: 'corr-1',
          causationId: 'cause-1',
        }),
      });

      expect(result.id).toBe(entity.id);
      expect(result.status).toBe(OutboxStatus.PENDING);
      expect(loggerMock.info).toHaveBeenCalledWith(
        'Outbox record persisted successfully',
        expect.objectContaining({
          outboxEventId: entity.id,
          correlationId: 'corr-1',
          requestId: 'req-1',
          causationId: 'cause-1',
        }),
      );
    });
  });

  describe('findPending', () => {
    it('should retrieve pending or retrying outbox records', async () => {
      const dbModels = [
        {
          id: 'id-1',
          aggregateId: 'agg-1',
          aggregateType: 'Payment',
          eventType: 'PaymentInitiated',
          payload: {},
          status: OutboxStatus.PENDING,
          requestId: 'req-1',
          correlationId: 'corr-1',
          causationId: 'cause-1',
          createdAt: new Date(),
          publishedAt: null,
          retryCount: 0,
          traceHeaders: null,
        },
      ];

      prismaMock.client.outboxEvent.findMany.mockResolvedValue(dbModels);

      const result = await repository.findPending(10);

      expect(prismaMock.client.outboxEvent.findMany).toHaveBeenCalledWith({
        where: {
          status: {
            in: [OutboxStatus.PENDING, OutboxStatus.RETRYING],
          },
        },
        orderBy: {
          createdAt: 'asc',
        },
        take: 10,
      });

      expect(result).toHaveLength(1);
      expect(result[0]!.id).toBe('id-1');
    });
  });

  describe('markPublished', () => {
    it('should transition status to PUBLISHED and record published timestamp', async () => {
      const dbModel = {
        id: 'id-1',
        aggregateId: 'agg-1',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: {},
        status: OutboxStatus.PUBLISHED,
        requestId: 'req-1',
        correlationId: 'corr-1',
        causationId: 'cause-1',
        createdAt: new Date(),
        publishedAt: new Date(),
        retryCount: 0,
        traceHeaders: null,
      };

      prismaMock.client.outboxEvent.update.mockResolvedValue(dbModel);

      const result = await repository.markPublished('id-1');

      expect(prismaMock.client.outboxEvent.update).toHaveBeenCalledWith({
        where: { id: 'id-1' },
        data: expect.objectContaining({
          status: OutboxStatus.PUBLISHED,
          publishedAt: expect.any(Date),
        }),
      });

      expect(result.status).toBe(OutboxStatus.PUBLISHED);
      expect(result.publishedAt).not.toBeNull();
    });
  });

  describe('incrementRetry', () => {
    it('should increment retryCount and set status to RETRYING', async () => {
      const dbModel = {
        id: 'id-1',
        aggregateId: 'agg-1',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: {},
        status: OutboxStatus.RETRYING,
        requestId: 'req-1',
        correlationId: 'corr-1',
        causationId: 'cause-1',
        createdAt: new Date(),
        publishedAt: null,
        retryCount: 1,
        traceHeaders: null,
      };

      prismaMock.client.outboxEvent.update.mockResolvedValue(dbModel);

      const result = await repository.incrementRetry('id-1');

      expect(prismaMock.client.outboxEvent.update).toHaveBeenCalledWith({
        where: { id: 'id-1' },
        data: {
          retryCount: {
            increment: 1,
          },
          status: OutboxStatus.RETRYING,
        },
      });

      expect(result.status).toBe(OutboxStatus.RETRYING);
      expect(result.retryCount).toBe(1);
    });
  });

  describe('markFailed', () => {
    it('should transition status to FAILED', async () => {
      const dbModel = {
        id: 'id-1',
        aggregateId: 'agg-1',
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: {},
        status: OutboxStatus.FAILED,
        requestId: 'req-1',
        correlationId: 'corr-1',
        causationId: 'cause-1',
        createdAt: new Date(),
        publishedAt: null,
        retryCount: 0,
        traceHeaders: null,
      };

      prismaMock.client.outboxEvent.update.mockResolvedValue(dbModel);

      const result = await repository.markFailed('id-1');

      expect(prismaMock.client.outboxEvent.update).toHaveBeenCalledWith({
        where: { id: 'id-1' },
        data: {
          status: OutboxStatus.FAILED,
        },
      });

      expect(result.status).toBe(OutboxStatus.FAILED);
    });
  });
});
