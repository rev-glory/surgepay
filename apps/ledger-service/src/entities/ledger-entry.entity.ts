import * as crypto from 'crypto';

import type { LedgerEntryType } from '../generated/client';

export class LedgerEntryEntity {
  constructor(
    public readonly id: string,
    public readonly paymentId: string,
    public readonly merchantId: string,
    public readonly amount: number,
    public readonly currency: string,
    public readonly entryType: LedgerEntryType,
    public readonly description: string,
    public readonly createdAt: Date,
    public readonly sourceCommandId: string,
    public readonly correlationId: string,
    public readonly causationId: string,
    public readonly sagaId: string,
    // Null for original entries. Non-null for compensation entries only.
    // References the UUID of the original DEBIT entry this CREDIT entry offsets.
    // Protected at the DB level by a partial unique index (WHERE "reversalOf" IS NOT NULL).
    public readonly reversalOf: string | null = null
  ) {}

  static create(params: {
    paymentId: string;
    merchantId: string;
    amount: number;
    currency: string;
    entryType: LedgerEntryType;
    description: string;
    sourceCommandId: string;
    correlationId: string;
    causationId: string;
    sagaId: string;
    reversalOf?: string | null;
  }): LedgerEntryEntity {
    return new LedgerEntryEntity(
      crypto.randomUUID(),
      params.paymentId,
      params.merchantId,
      params.amount,
      params.currency,
      params.entryType,
      params.description,
      new Date(),
      params.sourceCommandId,
      params.correlationId,
      params.causationId,
      params.sagaId,
      params.reversalOf ?? null
    );
  }
}
