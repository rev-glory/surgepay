import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface PaymentAlreadyCompletedMetadata extends Record<string, unknown> {
  paymentId: string;
}

export class PaymentAlreadyCompletedException extends DomainException<PaymentAlreadyCompletedMetadata> {
  constructor(paymentId: string, options?: { cause?: Error }) {
    super(
      `Payment with ID ${paymentId} is already completed.`,
      DomainErrorCode.PAYMENT_ALREADY_COMPLETED,
      409,
      { paymentId },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
