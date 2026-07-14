import { Injectable } from '@nestjs/common';

import { LedgerEntryEntity } from '../entities/ledger-entry.entity';
import { LedgerEntry, Prisma, PrismaClient } from '../generated/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class LedgerRepository {
  constructor(private readonly prisma: PrismaService) {}

  private getClient(tx?: Prisma.TransactionClient): Prisma.TransactionClient | PrismaClient {
    return tx || this.prisma.client;
  }

  async create(entity: LedgerEntryEntity, tx?: Prisma.TransactionClient): Promise<LedgerEntryEntity> {
    const client = this.getClient(tx);
    const record = await client.ledgerEntry.create({
      data: {
        id: entity.id,
        paymentId: entity.paymentId,
        merchantId: entity.merchantId,
        amount: entity.amount,
        currency: entity.currency,
        entryType: entity.entryType,
        description: entity.description,
        sourceCommandId: entity.sourceCommandId,
        correlationId: entity.correlationId,
        causationId: entity.causationId,
        sagaId: entity.sagaId,
        createdAt: entity.createdAt,
        // Persist reversalOf (null for original entries, UUID for compensation entries)
        reversalOf: entity.reversalOf ?? null,
      },
    });
    return this.mapToEntity(record);
  }

  async findBySourceCommandId(sourceCommandId: string, tx?: Prisma.TransactionClient): Promise<LedgerEntryEntity | null> {
    const client = this.getClient(tx);
    const record = await client.ledgerEntry.findUnique({
      where: { sourceCommandId },
    });
    return record ? this.mapToEntity(record) : null;
  }

  async findById(id: string, tx?: Prisma.TransactionClient): Promise<LedgerEntryEntity | null> {
    const client = this.getClient(tx);
    const record = await client.ledgerEntry.findUnique({
      where: { id },
    });
    return record ? this.mapToEntity(record) : null;
  }

  async findByPaymentId(paymentId: string, tx?: Prisma.TransactionClient): Promise<LedgerEntryEntity[]> {
    const client = this.getClient(tx);
    const records = await client.ledgerEntry.findMany({
      where: { paymentId },
    });
    return records.map((r) => this.mapToEntity(r));
  }

  /**
   * Finds the original (non-compensation) DEBIT entry for a payment.
   * An original entry is one with reversalOf = NULL.
   * Returns null if no original entry exists for the payment.
   */
  async findOriginalByPaymentId(
    paymentId: string,
    tx?: Prisma.TransactionClient
  ): Promise<LedgerEntryEntity | null> {
    const client = this.getClient(tx);
    const record = await client.ledgerEntry.findFirst({
      where: {
        paymentId,
        reversalOf: null,
      },
      orderBy: { createdAt: 'asc' },
    });
    return record ? this.mapToEntity(record) : null;
  }

  /**
   * Finds the compensation (reversal) entry for a given original entry ID.
   * Used to check idempotency before appending a new compensation entry.
   * The partial unique index on reversalOf guarantees at most one result.
   */
  async findCompensationByOriginalEntryId(
    originalEntryId: string,
    tx?: Prisma.TransactionClient
  ): Promise<LedgerEntryEntity | null> {
    const client = this.getClient(tx);
    const record = await client.ledgerEntry.findFirst({
      where: { reversalOf: originalEntryId },
    });
    return record ? this.mapToEntity(record) : null;
  }

  private mapToEntity(record: LedgerEntry): LedgerEntryEntity {
    return new LedgerEntryEntity(
      record.id,
      record.paymentId,
      record.merchantId,
      record.amount,
      record.currency,
      record.entryType,
      record.description,
      record.createdAt,
      record.sourceCommandId,
      record.correlationId,
      record.causationId,
      record.sagaId,
      record.reversalOf ?? null
    );
  }
}
