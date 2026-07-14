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

  async findById(id: string): Promise<LedgerEntryEntity | null> {
    const record = await this.prisma.client.ledgerEntry.findUnique({
      where: { id },
    });
    return record ? this.mapToEntity(record) : null;
  }

  async findByPaymentId(paymentId: string): Promise<LedgerEntryEntity[]> {
    const records = await this.prisma.client.ledgerEntry.findMany({
      where: { paymentId },
    });
    return records.map((r) => this.mapToEntity(r));
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
      record.sagaId
    );
  }
}
