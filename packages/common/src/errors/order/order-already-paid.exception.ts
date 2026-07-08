import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface OrderAlreadyPaidMetadata extends Record<string, unknown> {
  reference: string;
}

export class OrderAlreadyPaidException extends DomainException<OrderAlreadyPaidMetadata> {
  constructor(reference: string, options?: { cause?: Error }) {
    super(
      `Order with reference '${reference}' is already paid.`,
      DomainErrorCode.ORDER_ALREADY_PAID,
      409,
      { reference },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
