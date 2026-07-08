import { randomUUID } from 'crypto';

import { OrderStatus } from '../generated/client';

export class OrderEntity {
  constructor(
    public readonly id: string,
    public readonly merchantId: string,
    public readonly amount: number,
    public readonly currency: string,
    public status: OrderStatus,
    public readonly reference: string,
    public readonly createdAt: Date,
    public readonly updatedAt: Date,
  ) {}

  /**
   * Static factory to initialize a new Order aggregate root.
   * Every newly created order begins with CREATED status.
   */
  static create(params: {
    merchantId: string;
    amount: number;
    currency: string;
    reference: string;
  }): OrderEntity {
    return new OrderEntity(
      randomUUID(),
      params.merchantId,
      params.amount,
      params.currency,
      OrderStatus.CREATED,
      params.reference,
      new Date(),
      new Date(),
    );
  }

  /**
   * Encapsulates status transition invariants.
   * Order records support controlled status transitions:
   * - CREATED -> PAID | CANCELLED
   * - PAID -> REFUNDED
   * - CANCELLED and REFUNDED are terminal states
   */
  transitionTo(newStatus: OrderStatus): void {
    const currentStatus = this.status;

    if (currentStatus === newStatus) {
      return;
    }

    if (currentStatus === OrderStatus.CANCELLED || currentStatus === OrderStatus.REFUNDED) {
      throw new Error(`Cannot transition from terminal state ${currentStatus} to ${newStatus}`);
    }

    if (currentStatus === OrderStatus.CREATED) {
      if (newStatus !== OrderStatus.PAID && newStatus !== OrderStatus.CANCELLED) {
        throw new Error(`Illegal state transition from ${currentStatus} to ${newStatus}`);
      }
    } else if (currentStatus === OrderStatus.PAID) {
      if (newStatus !== OrderStatus.REFUNDED) {
        throw new Error(`Illegal state transition from ${currentStatus} to ${newStatus}`);
      }
    }

    this.status = newStatus;
  }
}
