import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface PaymentValidationMetadata extends Record<string, unknown> {
  errors?: unknown;
}

export class PaymentValidationException extends DomainException<PaymentValidationMetadata> {
  constructor(message: string, errors?: unknown, options?: { cause?: Error }) {
    super(
      message,
      DomainErrorCode.PAYMENT_VALIDATION_FAILED,
      400,
      { errors },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
