import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface OrderAmountMismatchMetadata extends Record<string, unknown> {
  reference: string;
  expectedAmount: number;
  requestedAmount: number;
}

export class OrderAmountMismatchException extends DomainException<OrderAmountMismatchMetadata> {
  constructor(reference: string, expectedAmount: number, requestedAmount: number, options?: { cause?: Error }) {
    super(
      `Amount mismatch: Order amount is ${expectedAmount}, requested amount is ${requestedAmount}.`,
      DomainErrorCode.ORDER_AMOUNT_MISMATCH,
      422,
      { reference, expectedAmount, requestedAmount },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
