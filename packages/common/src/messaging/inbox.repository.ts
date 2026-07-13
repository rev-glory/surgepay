import type { BaseEventEnvelope } from '@surgepay/events';

export type InboxStatus = 'RECEIVED' | 'PROCESSING' | 'PROCESSED' | 'FAILED' | 'RETRYING' | 'DLQ_SENT';

export interface InboxEvent {
  id: string;
  eventId: string;
  consumer: string;
  status: InboxStatus;
  payload: unknown;
  receivedAt: Date;
  processedAt: Date | null;
  retryCount: number;
  eventType: string;
  correlationId: string;
  causationId: string;
  sagaId: string;
  timestamp: Date;
  version: number;
}

export interface PrismaInboxDelegate {
  create(args: { data: unknown }): Promise<unknown>;
  findUnique(args: {
    where: {
      consumer_eventId: {
        consumer: string;
        eventId: string;
      };
    };
  }): Promise<unknown>;
  update(args: {
    where: {
      consumer_eventId: {
        consumer: string;
        eventId: string;
      };
    };
    data: {
      status: InboxStatus;
      processedAt?: Date | null;
      retryCount?: number;
    };
  }): Promise<unknown>;
  updateMany(args: {
    where: {
      consumer: string;
      eventId: string;
      status: { in: InboxStatus[] };
    };
    data: {
      status: InboxStatus;
      retryCount?: number;
    };
  }): Promise<{ count: number }>;
}

export interface PrismaClientLike {
  inboxEvent: PrismaInboxDelegate;
}

export abstract class BaseInboxRepository {
  protected constructor(protected readonly prismaClient: PrismaClientLike) {}

  async recordReceived(
    envelope: BaseEventEnvelope<unknown>,
    consumer: string,
  ): Promise<InboxEvent> {
    const model = await this.prismaClient.inboxEvent.create({
      data: {
        eventId: envelope.eventId,
        consumer,
        status: 'RECEIVED',
        payload: envelope.payload as Record<string, unknown>,
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        causationId: envelope.causationId,
        sagaId: envelope.sagaId,
        timestamp: new Date(envelope.timestamp),
        version: envelope.version,
      },
    });

    return model as InboxEvent;
  }

  async findByEventIdAndConsumer(
    eventId: string,
    consumer: string,
  ): Promise<InboxEvent | null> {
    const model = await this.prismaClient.inboxEvent.findUnique({
      where: {
        consumer_eventId: {
          consumer,
          eventId,
        },
      },
    });

    return (model as InboxEvent) || null;
  }

  async transitionStatus(
    eventId: string,
    consumer: string,
    fromStatus: InboxStatus | InboxStatus[],
    toStatus: InboxStatus,
  ): Promise<InboxEvent | null> {
    const statuses = Array.isArray(fromStatus) ? fromStatus : [fromStatus];
    const result = await this.prismaClient.inboxEvent.updateMany({
      where: {
        consumer,
        eventId,
        status: { in: statuses },
      },
      data: {
        status: toStatus,
      },
    });

    if (result.count === 0) {
      return null;
    }

    return this.findByEventIdAndConsumer(eventId, consumer);
  }

  async updateStatus(
    eventId: string,
    consumer: string,
    status: InboxStatus,
    retryCount?: number,
  ): Promise<InboxEvent> {
    const model = await this.prismaClient.inboxEvent.update({
      where: {
        consumer_eventId: {
          consumer,
          eventId,
        },
      },
      data: {
        status,
        processedAt: status === 'PROCESSED' ? new Date() : undefined,
        retryCount: retryCount !== undefined ? retryCount : undefined,
      },
    });

    return model as InboxEvent;
  }

  async prepareForReplay(eventId: string, consumer: string): Promise<boolean> {
    const result = await this.prismaClient.inboxEvent.updateMany({
      where: {
        consumer,
        eventId,
        status: { in: ['DLQ_SENT'] },
      },
      data: {
        status: 'RETRYING',
        retryCount: 0,
      },
    });

    return result.count > 0;
  }
}
