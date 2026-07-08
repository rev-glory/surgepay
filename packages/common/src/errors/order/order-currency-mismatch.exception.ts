import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface OrderCurrencyMismatchMetadata extends Record<string, unknown> {
  reference: string;
  expectedCurrency: string;
  requestedCurrency: string;
}

export class OrderCurrencyMismatchException extends DomainException<OrderCurrencyMismatchMetadata> {
  constructor(reference: string, expectedCurrency: string, requestedCurrency: string, options?: { cause?: Error }) {
    super(
      `Currency mismatch: expected ${expectedCurrency}, requested ${requestedCurrency}.`,
      DomainErrorCode.ORDER_CURRENCY_MISMATCH,
      422,
      { reference, expectedCurrency, requestedCurrency },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
