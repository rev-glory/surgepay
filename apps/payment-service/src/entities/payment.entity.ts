import { randomUUID } from 'crypto';

import { PaymentStateMachine } from '../domain/payment-state-machine';
import { PaymentStatus } from '../generated/client';

export class PaymentEntity {
  private _status: PaymentStatus;

  constructor(
    public readonly id: string,
    public readonly merchantId: string,
    public readonly amount: number,
    public readonly currency: string,
    status: PaymentStatus,
    public readonly reference: string,
    public readonly requestId: string,
    public readonly correlationId: string,
    public readonly causationId: string,
    public readonly createdBy: string,
    public readonly source: string,
    public readonly createdAt: Date,
    public readonly updatedAt: Date,
  ) {
    this._status = status;
  }

  get status(): PaymentStatus {
    return this._status;
  }

  /**
   * Static factory to initialize a new Payment aggregate root.
   * Every newly created payment begins with PENDING.
   */
  static create(params: {
    merchantId: string;
    amount: number;
    currency: string;
    reference: string;
    requestId: string;
    correlationId: string;
    causationId: string;
    createdBy: string;
    source: string;
  }): PaymentEntity {
    return new PaymentEntity(
      randomUUID(),
      params.merchantId,
      params.amount,
      params.currency,
      PaymentStatus.PENDING,
      params.reference,
      params.requestId,
      params.correlationId,
      params.causationId,
      params.createdBy,
      params.source,
      new Date(),
      new Date(),
    );
  }

  /**
   * Transition the payment status using the PaymentStateMachine.
   */
  transitionTo(newStatus: PaymentStatus): void {
    const previousStatus = this._status;
    this._status = PaymentStateMachine.transition(this.id, previousStatus, newStatus);
  }
}
