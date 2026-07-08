import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface MerchantOwnershipMetadata extends Record<string, unknown> {
  reference: string;
  merchantId: string;
}

export class MerchantOwnershipException extends DomainException<MerchantOwnershipMetadata> {
  constructor(reference: string, merchantId: string, options?: { cause?: Error }) {
    super(
      'Merchant does not own this order',
      DomainErrorCode.ORDER_MERCHANT_MISMATCH,
      403,
      { reference, merchantId },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
