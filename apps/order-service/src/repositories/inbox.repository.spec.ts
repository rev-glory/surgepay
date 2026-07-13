import type { BaseEventEnvelope } from '@surgepay/events';

import type { PrismaService } from '../prisma/prisma.service';
import { OrderInboxRepository } from './inbox.repository';

describe('OrderInboxRepository', () => {
  let repository: OrderInboxRepository;
  let prismaMock: {
    client: {
      inboxEvent: {
        create: jest.Mock;
        findUnique: jest.Mock;
      };
    };
  };

  beforeEach(() => {
    prismaMock = {
      client: {
        inboxEvent: {
          create: jest.fn(),
          findUnique: jest.fn(),
        },
      },
    };

    repository = new OrderInboxRepository(prismaMock as unknown as PrismaService);
  });

  describe('recordReceived', () => {
    it('should create and return a RECEIVED inbox record', async () => {
      const envelope: BaseEventEnvelope<unknown> = {
        eventId: 'event-123',
        eventType: 'PaymentInitiated',
        correlationId: 'corr-123',
        causationId: 'cause-123',
        sagaId: 'saga-123',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: { amount: 100 },
      };

      const dbModel = {
        id: 'id-uuid',
        eventId: envelope.eventId,
        consumer: 'order-saga-orchestrator',
        status: 'RECEIVED',
        payload: envelope.payload,
        receivedAt: new Date(),
        processedAt: null,
        retryCount: 0,
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        causationId: envelope.causationId,
        sagaId: envelope.sagaId,
        timestamp: new Date(envelope.timestamp),
        version: envelope.version,
      };

      prismaMock.client.inboxEvent.create.mockResolvedValue(dbModel);

      const result = await repository.recordReceived(envelope, 'order-saga-orchestrator');

      expect(prismaMock.client.inboxEvent.create).toHaveBeenCalledWith({
        data: expect.objectContaining({
          eventId: envelope.eventId,
          consumer: 'order-saga-orchestrator',
          status: 'RECEIVED',
          eventType: envelope.eventType,
          correlationId: envelope.correlationId,
          causationId: envelope.causationId,
          sagaId: envelope.sagaId,
          version: envelope.version,
        }),
      });

      expect(result.eventId).toBe(envelope.eventId);
      expect(result.status).toBe('RECEIVED');
    });
  });

  describe('findByEventIdAndConsumer', () => {
    it('should query inbox records by eventId and consumer group key', async () => {
      const dbModel = {
        id: 'id-uuid',
        eventId: 'event-123',
        consumer: 'order-saga-orchestrator',
        status: 'RECEIVED',
        payload: {},
        receivedAt: new Date(),
        processedAt: null,
        retryCount: 0,
        eventType: 'PaymentInitiated',
        correlationId: 'corr-123',
        causationId: 'cause-123',
        sagaId: 'saga-123',
        timestamp: new Date(),
        version: 1,
      };

      prismaMock.client.inboxEvent.findUnique.mockResolvedValue(dbModel);

      const result = await repository.findByEventIdAndConsumer('event-123', 'order-saga-orchestrator');

      expect(prismaMock.client.inboxEvent.findUnique).toHaveBeenCalledWith({
        where: {
          consumer_eventId: {
            consumer: 'order-saga-orchestrator',
            eventId: 'event-123',
          },
        },
      });

      expect(result?.eventId).toBe('event-123');
    });
  });
});
