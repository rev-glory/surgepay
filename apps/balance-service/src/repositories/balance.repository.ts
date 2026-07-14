import { Injectable } from '@nestjs/common';

import { MerchantBalanceEntity } from '../entities/merchant-balance.entity';
import { MerchantBalance, Prisma, PrismaClient } from '../generated/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class BalanceRepository {
  constructor(private readonly prisma: PrismaService) {}

  private getClient(tx?: Prisma.TransactionClient): Prisma.TransactionClient | PrismaClient {
    return tx || this.prisma.client;
  }

  async create(entity: MerchantBalanceEntity, tx?: Prisma.TransactionClient): Promise<MerchantBalanceEntity> {
    const client = this.getClient(tx);
    const record = await client.merchantBalance.create({
      data: {
        id: entity.id,
        merchantId: entity.merchantId,
        currency: entity.currency,
        available: entity.available,
        reserved: entity.reserved,
      },
    });
    return this.mapToEntity(record);
  }

  async findByMerchantId(merchantId: string, tx?: Prisma.TransactionClient): Promise<MerchantBalanceEntity[]> {
    const client = this.getClient(tx);
    const records = await client.merchantBalance.findMany({
      where: { merchantId },
    });
    return records.map((r) => this.mapToEntity(r));
  }

  async findByMerchantIdAndCurrency(
    merchantId: string,
    currency: string,
    tx?: Prisma.TransactionClient
  ): Promise<MerchantBalanceEntity | null> {
    const client = this.getClient(tx);
    const record = await client.merchantBalance.findUnique({
      where: {
        merchantId_currency: {
          merchantId,
          currency,
        },
      },
    });
    return record ? this.mapToEntity(record) : null;
  }

  /**
   * Performs an atomic database conditional update to reserve merchant funds.
   * Decrements available and increments reserved by requested amount under read committed
   * isolation level, serialized by PostgreSQL database-level write locks.
   *
   * @returns true if reservation succeeded (1 row updated), false otherwise (0 rows updated)
   */
  async reserveFunds(
    merchantId: string,
    currency: string,
    amount: number,
    tx?: Prisma.TransactionClient
  ): Promise<boolean> {
    const client = this.getClient(tx);
    const result = await client.merchantBalance.updateMany({
      where: {
        merchantId,
        currency,
        available: {
          gte: amount,
        },
      },
      data: {
        available: {
          decrement: amount,
        },
        reserved: {
          increment: amount,
        },
      },
    });
    return result.count === 1;
  }

  private mapToEntity(record: MerchantBalance): MerchantBalanceEntity {
    return new MerchantBalanceEntity(
      record.id,
      record.merchantId,
      record.currency,
      record.available,
      record.reserved,
      record.updatedAt
    );
  }
}
