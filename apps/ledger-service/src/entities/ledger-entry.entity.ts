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
    public readonly sagaId: string
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
      params.sagaId
    );
  }
}
