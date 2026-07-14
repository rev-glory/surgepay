import { Injectable } from '@nestjs/common';

import { MerchantBalanceEntity } from '../entities/merchant-balance.entity';
import { BalanceReversal, MerchantBalance, Prisma, PrismaClient } from '../generated/client';
import { PrismaService } from '../prisma/prisma.service';

export interface BalanceReversalRecord {
  id: string;
  paymentId: string;
  merchantId: string;
  currency: string;
  amount: number;
  commandId: string;
  reversedAt: Date;
}

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

  /**
   * Atomically releases a reserved amount back to available.
   * Decrements reserved and increments available by the requested amount.
   * Guarded by reserved >= amount to prevent negative reserved values.
   *
   * @returns true if the release succeeded (1 row updated), false if no row matched
   */
  async releaseFunds(
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
        reserved: {
          gte: amount,
        },
      },
      data: {
        reserved: {
          decrement: amount,
        },
        available: {
          increment: amount,
        },
      },
    });
    return result.count === 1;
  }

  /**
   * Finds a BalanceReversal record by paymentId.
   * Returns null if no reversal has been processed for this payment.
   * Used to check business-level idempotency before applying releaseFunds.
   */
  async findReversalByPaymentId(
    paymentId: string,
    tx?: Prisma.TransactionClient
  ): Promise<BalanceReversalRecord | null> {
    const client = this.getClient(tx);
    const record = await client.balanceReversal.findUnique({
      where: { paymentId },
    });
    return record ? this.mapReversalToRecord(record) : null;
  }

  /**
   * Creates a BalanceReversal audit record inside the caller's transaction.
   * The paymentId @unique constraint enforces that only one reversal can ever
   * succeed per payment. If a concurrent insert wins, the caller catches the
   * P2002 constraint error and treats it as an idempotent skip.
   */
  async createReversal(
    data: {
      paymentId: string;
      merchantId: string;
      currency: string;
      amount: number;
      commandId: string;
    },
    tx?: Prisma.TransactionClient
  ): Promise<BalanceReversalRecord> {
    const client = this.getClient(tx);
    const record = await client.balanceReversal.create({
      data: {
        paymentId: data.paymentId,
        merchantId: data.merchantId,
        currency: data.currency,
        amount: data.amount,
        commandId: data.commandId,
      },
    });
    return this.mapReversalToRecord(record);
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

  private mapReversalToRecord(record: BalanceReversal): BalanceReversalRecord {
    return {
      id: record.id,
      paymentId: record.paymentId,
      merchantId: record.merchantId,
      currency: record.currency,
      amount: record.amount,
      commandId: record.commandId,
      reversedAt: record.reversedAt,
    };
  }
}
