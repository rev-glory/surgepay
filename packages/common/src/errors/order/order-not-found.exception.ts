import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface OrderNotFoundMetadata extends Record<string, unknown> {
  reference: string;
}

export class OrderNotFoundException extends DomainException<OrderNotFoundMetadata> {
  constructor(reference: string, options?: { cause?: Error }) {
    super(
      `Order with reference '${reference}' not found.`,
      DomainErrorCode.ORDER_NOT_FOUND,
      404,
      { reference },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
