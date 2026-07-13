import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';

import { OutboxEvent, OutboxStatus } from '../generated/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class OutboxRepository {
  constructor(
    private readonly prisma: PrismaService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('OutboxRepository');
  }

  /**
   * Selects eligible pending or retrying outbox events, updates their status to PUBLISHING,
   * sets the attempt timestamp, and commits the transaction to release the PostgreSQL row locks.
   */
  async claimPending(batchSize: number): Promise<OutboxEvent[]> {
    return this.prisma.client.$transaction(async (tx) => {
      const sql = `
        SELECT id FROM "payment"."OutboxEvent"
        WHERE status IN ('PENDING', 'RETRYING')
        ORDER BY "createdAt" ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
      `;
      const rows = await tx.$queryRawUnsafe<{ id: string }[]>(sql, batchSize);
      if (rows.length === 0) {
        return [];
      }

      const ids = rows.map((r) => r.id);

      await tx.outboxEvent.updateMany({
        where: { id: { in: ids } },
        data: {
          status: OutboxStatus.PUBLISHING,
          lastAttemptAt: new Date(),
        },
      });

      return tx.outboxEvent.findMany({
        where: { id: { in: ids } },
        orderBy: { createdAt: 'asc' },
      });
    });
  }

  /**
   * Atomically transitions an outbox event from PUBLISHING to PUBLISHED and stores Kafka broker metadata.
   */
  async markPublished(id: string, partition: number, offset: string): Promise<OutboxEvent> {
    try {
      const result = await this.prisma.client.outboxEvent.update({
        where: {
          id,
          status: OutboxStatus.PUBLISHING,
        },
        data: {
          status: OutboxStatus.PUBLISHED,
          publishedAt: new Date(),
          partition,
          offset,
        },
      });

      this.logger.info('Outbox event marked as PUBLISHED successfully', {
        eventId: id,
        partition,
        offset,
      });

      return result;
    } catch (err) {
      this.logger.error('Failed to transition outbox event to PUBLISHED. State conflict or missing record.', err, { eventId: id });
      throw err;
    }
  }

  /**
   * Increments the retry count and transitions status from PUBLISHING to FAILED.
   */
  async markFailed(id: string, errorMsg: string): Promise<OutboxEvent> {
    try {
      const result = await this.prisma.client.outboxEvent.update({
        where: {
          id,
          status: OutboxStatus.PUBLISHING,
        },
        data: {
          status: OutboxStatus.FAILED,
          retryCount: {
            increment: 1,
          },
        },
      });

      this.logger.warn('Outbox event transitioned to FAILED state', {
        eventId: id,
        error: errorMsg,
        retryCount: result.retryCount,
      });

      return result;
    } catch (err) {
      this.logger.error('Failed to transition outbox event to FAILED. State conflict or missing record.', err, { eventId: id });
      throw err;
    }
  }

  /**
   * Re-promotes an event from FAILED to RETRYING to make it eligible for another publication attempt.
   */
  async markRetrying(id: string): Promise<OutboxEvent> {
    try {
      const result = await this.prisma.client.outboxEvent.update({
        where: {
          id,
          status: OutboxStatus.FAILED,
        },
        data: {
          status: OutboxStatus.RETRYING,
        },
      });

      this.logger.info('Outbox event promoted from FAILED to RETRYING state', {
        eventId: id,
        retryCount: result.retryCount,
      });

      return result;
    } catch (err) {
      this.logger.error('Failed to transition outbox event to RETRYING. State conflict or missing record.', err, { eventId: id });
      throw err;
    }
  }

  /**
   * Concurrency-safe atomic stale claim recovery.
   * Locks stale events, increments their retryCount, and transitions status.
   */
  async recoverStale(staleTimeoutMs: number, retryLimit: number): Promise<void> {
    const staleThreshold = new Date(Date.now() - staleTimeoutMs);
    
    await this.prisma.client.$transaction(async (tx) => {
      const sql = `
        SELECT id FROM "payment"."OutboxEvent"
        WHERE status = 'PUBLISHING'
          AND "lastAttemptAt" < $1
        ORDER BY "createdAt" ASC
        FOR UPDATE SKIP LOCKED
      `;
      const rows = await tx.$queryRawUnsafe<{ id: string }[]>(sql, staleThreshold);
      if (rows.length === 0) {
        return;
      }

      const ids = rows.map((r) => r.id);
      
      const events = await tx.outboxEvent.findMany({
        where: { id: { in: ids } },
      });

      for (const event of events) {
        const nextRetryCount = event.retryCount + 1;
        const newStatus = nextRetryCount < retryLimit ? OutboxStatus.RETRYING : OutboxStatus.FAILED;

        await tx.outboxEvent.update({
          where: { id: event.id },
          data: {
            status: newStatus,
            retryCount: nextRetryCount,
          },
        });

        this.logger.warn('Stale outbox event recovered', {
          eventId: event.id,
          eventType: event.eventType,
          previousStatus: event.status,
          newStatus,
          retryCount: nextRetryCount,
          retryLimit,
        });
      }
    });
  }
}
