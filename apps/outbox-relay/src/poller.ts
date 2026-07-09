import { Injectable } from '@nestjs/common';

import { OutboxEvent, Prisma } from '../../payment-service/src/generated/client';

@Injectable()
export class Poller {
  /**
   * Retrieves pending outbox events while acquiring a FOR UPDATE SKIP LOCKED row lock.
   * Must be executed within a database transaction context to keep locks active.
   */
  async pollPending(
    tx: Prisma.TransactionClient,
    batchSize: number,
  ): Promise<OutboxEvent[]> {
    // We execute a raw SQL query because FOR UPDATE SKIP LOCKED is not supported by Prisma's standard API
    // Quoting column and table names to handle mixed-case names correctly in PostgreSQL
    return tx.$queryRawUnsafe<OutboxEvent[]>(
      `SELECT * FROM "OutboxEvent" WHERE "status" = 'PENDING' ORDER BY "createdAt" ASC LIMIT $1 FOR UPDATE SKIP LOCKED`,
      batchSize,
    );
  }
}
