import { BaseError } from '@surgepay/common';
import { PlatformErrorCode } from '@surgepay/contracts';

import type { PaymentStatus } from '../../generated/client';

export class InvalidPaymentStateTransitionException extends BaseError {
  readonly statusCode = 422;
  readonly code = PlatformErrorCode.INVALID_REQUEST;

  constructor(
    public readonly paymentId: string,
    public readonly currentStatus: PaymentStatus,
    public readonly attemptedStatus: PaymentStatus,
    reason: string,
  ) {
    super(
      `Invalid payment state transition for payment ${paymentId}: cannot transition from ${currentStatus} to ${attemptedStatus}. Reason: ${reason}`,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
