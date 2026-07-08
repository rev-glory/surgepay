import { InvalidPaymentStateTransitionException } from '@surgepay/common';

import { PaymentStatus } from '../generated/client';

export class PaymentStateMachine {
  private static readonly ALLOWED_TRANSITIONS: Record<PaymentStatus, Set<PaymentStatus>> = {
    [PaymentStatus.PENDING]: new Set([PaymentStatus.PROCESSING]),
    [PaymentStatus.PROCESSING]: new Set([PaymentStatus.COMPLETED, PaymentStatus.FAILED]),
    [PaymentStatus.COMPLETED]: new Set(),
    [PaymentStatus.FAILED]: new Set(),
  };

  static transition(
    paymentId: string,
    currentStatus: PaymentStatus,
    attemptedStatus: PaymentStatus,
  ): PaymentStatus {

    const allowed = this.ALLOWED_TRANSITIONS[currentStatus];
    if (!allowed || !allowed.has(attemptedStatus)) {
      throw new InvalidPaymentStateTransitionException(
        paymentId,
        currentStatus,
        attemptedStatus,
        `Transition from ${currentStatus} to ${attemptedStatus} is not allowed.`,
      );
    }

    return attemptedStatus;
  }
}
