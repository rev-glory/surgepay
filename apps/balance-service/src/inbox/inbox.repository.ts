import { Injectable } from '@nestjs/common';
import { EventEnvelope, InboxEvent } from '@surgepay/events';
import { InboxPersister, DuplicateEventException } from '@surgepay/common-messaging';
import { Prisma } from '@surgepay/database/generated/balance';
import { PrismaService } from '../prisma.service';

@Injectable()
export class InboxRepository implements InboxPersister {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Finds an existing inbox event record matching the consumer group and event ID.
   */
  async find(consumer: string, eventId: string): Promise<InboxEvent | null> {
    const record = await this.prisma.client.inboxEvent.findUnique({
      where: {
        consumer_eventId: {
          consumer,
          eventId,
        },
      },
    });
    return record as InboxEvent | null;
  }

  /**
   * Persists a received event envelope into the Inbox database schema.
   */
  async persistReceived(envelope: EventEnvelope): Promise<InboxEvent> {
    try {
      const record = await this.prisma.client.inboxEvent.create({
        data: {
          eventId: envelope.eventId,
          consumer: 'balance-service',
          eventType: envelope.eventType,
          status: 'RECEIVED',
          payload: envelope.payload as any,
          correlationId: envelope.correlationId,
          causationId: envelope.causationId,
          sagaId: envelope.sagaId ?? null,
          receivedAt: new Date(),
          retryCount: 0,
        },
      });
      return record as InboxEvent;
    } catch (err) {
      if (err instanceof Prisma.PrismaClientKnownRequestError && err.code === 'P2002') {
        throw new DuplicateEventException(envelope.eventId, 'balance-service');
      }
      throw err;
    }
  }

  /**
   * Updates state to PROCESSING.
   */
  async markProcessing(id: string): Promise<void> {
    await this.prisma.client.inboxEvent.update({
      where: { id },
      data: {
        status: 'PROCESSING',
      },
    });
  }

  /**
   * Updates state to PROCESSED and records processed timestamp.
   */
  async markProcessed(id: string): Promise<void> {
    await this.prisma.client.inboxEvent.update({
      where: { id },
      data: {
        status: 'PROCESSED',
        processedAt: new Date(),
      },
    });
  }

  /**
   * Updates state to FAILED and records reason.
   */
  async markFailed(id: string, reason: string): Promise<void> {
    await this.prisma.client.inboxEvent.update({
      where: { id },
      data: {
        status: 'FAILED',
        failureReason: reason,
      },
    });
  }

  /**
   * Transitions to RETRYING and increments retryCount.
   */
  async markRetrying(id: string, reason: string): Promise<void> {
    await this.prisma.client.inboxEvent.update({
      where: { id },
      data: {
        status: 'RETRYING',
        retryCount: { increment: 1 },
        failureReason: reason,
      },
    });
  }

  /**
   * Find an inbox event by ID.
   */
  async findById(id: string): Promise<InboxEvent | null> {
    const record = await this.prisma.client.inboxEvent.findUnique({
      where: { id },
    });
    return record as InboxEvent | null;
  }
}
