import { randomUUID } from 'crypto';

export class MerchantBalanceEntity {
  constructor(
    public readonly id: string,
    public readonly merchantId: string,
    public readonly currency: string,
    public readonly available: number,
    public readonly reserved: number,
    public readonly updatedAt: Date
  ) {}

  static create(params: {
    merchantId: string;
    currency: string;
    available: number;
    reserved: number;
  }): MerchantBalanceEntity {
    return new MerchantBalanceEntity(
      randomUUID(),
      params.merchantId,
      params.currency,
      params.available,
      params.reserved,
      new Date()
    );
  }
}
