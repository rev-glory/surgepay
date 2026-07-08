import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface InvalidPaymentStateTransitionMetadata extends Record<string, unknown> {
  paymentId: string;
  currentStatus: string;
  attemptedStatus: string;
  reason?: string;
}

export class InvalidPaymentStateTransitionException extends DomainException<InvalidPaymentStateTransitionMetadata> {
  constructor(
    public readonly paymentId: string,
    public readonly currentStatus: string,
    public readonly attemptedStatus: string,
    public readonly reason?: string,
    options?: { cause?: Error },
  ) {
    super(
      `Invalid payment state transition for payment ${paymentId}: cannot transition from ${currentStatus} to ${attemptedStatus}.${reason ? ` Reason: ${reason}` : ''}`,
      DomainErrorCode.INVALID_PAYMENT_STATE_TRANSITION,
      422,
      { paymentId, currentStatus, attemptedStatus, reason },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
