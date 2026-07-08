import { randomUUID } from 'crypto';

import { PaymentStatus } from '../generated/client';

export class PaymentEntity {
  constructor(
    public readonly id: string,
    public readonly merchantId: string,
    public readonly amount: number,
    public readonly currency: string,
    public status: PaymentStatus,
    public readonly reference: string,
    public readonly createdAt: Date,
    public readonly updatedAt: Date,
  ) {}

  /**
   * Static factory to initialize a new Payment aggregate root.
   * Every newly created payment begins with PENDING.
   */
  static create(params: {
    merchantId: string;
    amount: number;
    currency: string;
    reference: string;
  }): PaymentEntity {
    return new PaymentEntity(
      randomUUID(),
      params.merchantId,
      params.amount,
      params.currency,
      PaymentStatus.PENDING,
      params.reference,
      new Date(),
      new Date(),
    );
  }

  /**
   * Prepare the aggregate for controlled status transitions.
   * Implement only the minimal transition support required for this commit;
   * the complete payment state machine will be introduced in Commit 7.
   */
  transitionTo(newStatus: PaymentStatus): void {
    const currentStatus = this.status;

    if (currentStatus === newStatus) {
      return;
    }

    // Minimal transition validation: prevent transitions from terminal states
    if (currentStatus === PaymentStatus.COMPLETED || currentStatus === PaymentStatus.FAILED) {
      throw new Error(`Cannot transition from terminal state ${currentStatus} to ${newStatus}`);
    }

    // PENDING can only go to PROCESSING
    if (currentStatus === PaymentStatus.PENDING && newStatus !== PaymentStatus.PROCESSING) {
      throw new Error(`Illegal state transition from ${currentStatus} to ${newStatus}`);
    }

    // PROCESSING can go to COMPLETED or FAILED
    if (
      currentStatus === PaymentStatus.PROCESSING &&
      newStatus !== PaymentStatus.COMPLETED &&
      newStatus !== PaymentStatus.FAILED
    ) {
      throw new Error(`Illegal state transition from ${currentStatus} to ${newStatus}`);
    }

    this.status = newStatus;
  }
}
