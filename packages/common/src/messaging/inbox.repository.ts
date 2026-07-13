import type { BaseEventEnvelope } from '@surgepay/events';

export type InboxStatus = 'RECEIVED' | 'PROCESSING' | 'PROCESSED' | 'FAILED' | 'RETRYING';

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
}
