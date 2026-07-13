import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';

import { OutboxEvent } from './generated/client';
import { PrismaService } from './prisma/prisma.service';

@Injectable()
export class OutboxPoller {
  constructor(
    private readonly prisma: PrismaService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('OutboxPoller');
  }

  /**
   * Polls database for pending outbox events using FOR UPDATE SKIP LOCKED.
   * Runs in a short-lived transaction to hold the exclusive lock, retrieve the rows,
   * and immediately release the lock when the transaction commits.
   */
  async pollPending(batchSize: number): Promise<OutboxEvent[]> {
    return this.prisma.client.$transaction(async (tx) => {
      // Execute raw query for FOR UPDATE SKIP LOCKED
      // Note: PostgreSQL requires double quotes around table and column names if they are mixed-case
      const sql = `
        SELECT id, "aggregateId", "aggregateType", "eventType", payload, status, "requestId", "correlationId", "causationId", "createdAt", "publishedAt", "retryCount"
        FROM "payment"."OutboxEvent"
        WHERE status = 'PENDING'
        ORDER BY "createdAt" ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
      `;
      
      const rows = await tx.$queryRawUnsafe<OutboxEvent[]>(sql, batchSize);
      return rows;
    });
  }
}
