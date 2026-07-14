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

  async countPending(): Promise<number> {
    return this.prisma.client.outboxEvent.count({
      where: {
        status: { in: [OutboxStatus.PENDING, OutboxStatus.RETRYING, OutboxStatus.PUBLISHING] },
      },
    });
  }

  async countFailed(): Promise<number> {
    return this.prisma.client.outboxEvent.count({
      where: {
        status: OutboxStatus.FAILED,
      },
    });
  }

  async countPublished(): Promise<number> {
    return this.prisma.client.outboxEvent.count({
      where: {
        status: OutboxStatus.PUBLISHED,
      },
    });
  }

  async markPublishedBatch(items: { id: string; partition: number; offset: string }[]): Promise<void> {
    if (items.length === 0) return;
    try {
      const sql = `
        UPDATE "payment"."OutboxEvent" as e
        SET 
          status = 'PUBLISHED',
          "publishedAt" = NOW(),
          partition = v.part,
          "offset" = v.off
        FROM (
          VALUES 
            ${items.map((_, idx) => `($${idx * 3 + 1}::uuid, $${idx * 3 + 2}::integer, $${idx * 3 + 3}::text)`).join(', ')}
        ) as v(id, part, off)
        WHERE e.id = v.id AND e.status = 'PUBLISHING'
      `;
      const params = items.flatMap((item) => [item.id, item.partition, item.offset]);
      await this.prisma.client.$executeRawUnsafe(sql, ...params);

      this.logger.info('Outbox events marked as PUBLISHED successfully (batch)', {
        count: items.length,
      });
    } catch (err) {
      this.logger.error('Failed to transition outbox events to PUBLISHED (batch). State conflict or missing record.', err);
      throw err;
    }
  }

  async markFailedBatch(ids: string[], errorMsg: string, retryLimit: number): Promise<void> {
    if (ids.length === 0) return;
    try {
      await this.prisma.client.$transaction(async (tx) => {
        // Step 1: Increment retryCount and transition to FAILED
        await tx.outboxEvent.updateMany({
          where: {
            id: { in: ids },
            status: OutboxStatus.PUBLISHING,
          },
          data: {
            status: OutboxStatus.FAILED,
            retryCount: {
              increment: 1,
            },
          },
        });

        // Step 2: Transition back to RETRYING for those below the retryLimit
        await tx.outboxEvent.updateMany({
          where: {
            id: { in: ids },
            status: OutboxStatus.FAILED,
            retryCount: {
              lt: retryLimit,
            },
          },
          data: {
            status: OutboxStatus.RETRYING,
          },
        });
      });

      this.logger.warn('Outbox events transitioned to FAILED/RETRYING states (batch)', {
        count: ids.length,
        error: errorMsg,
      });
    } catch (err) {
      this.logger.error('Failed to transition outbox events to FAILED/RETRYING (batch). State conflict.', err);
      throw err;
    }
  }
}
