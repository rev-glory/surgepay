import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface PaymentNotFoundMetadata extends Record<string, unknown> {
  paymentId: string;
}

export class PaymentNotFoundException extends DomainException<PaymentNotFoundMetadata> {
  constructor(paymentId: string, options?: { cause?: Error }) {
    super(
      `Payment with ID ${paymentId} not found.`,
      DomainErrorCode.PAYMENT_NOT_FOUND,
      404,
      { paymentId },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
