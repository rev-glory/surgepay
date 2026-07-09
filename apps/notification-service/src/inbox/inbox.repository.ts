import { Injectable } from '@nestjs/common';
import { EventEnvelope } from '@surgepay/events';
import { InboxPersister } from '@surgepay/common-messaging';
import { PrismaService } from '../prisma.service';

@Injectable()
export class InboxRepository implements InboxPersister {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Persists a received event envelope into the Inbox database schema.
   */
  async persist(envelope: EventEnvelope): Promise<void> {
    await this.prisma.client.inboxEvent.create({
      data: {
        eventId: envelope.eventId,
        consumer: 'notification-service',
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
  async findById(id: string) {
    return this.prisma.client.inboxEvent.findUnique({
      where: { id },
    });
  }

  /**
   * Find an inbox event by event ID.
   */
  async findByEventId(eventId: string) {
    return this.prisma.client.inboxEvent.findFirst({
      where: { eventId },
    });
  }
}
