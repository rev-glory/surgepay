import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface DuplicatePaymentReferenceMetadata extends Record<string, unknown> {
  merchantId: string;
  reference: string;
}

export class DuplicatePaymentReferenceException extends DomainException<DuplicatePaymentReferenceMetadata> {
  constructor(merchantId: string, reference: string, options?: { cause?: Error }) {
    super(
      'Payment reference already exists.',
      DomainErrorCode.DUPLICATE_PAYMENT_REFERENCE,
      409,
      { merchantId, reference },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
