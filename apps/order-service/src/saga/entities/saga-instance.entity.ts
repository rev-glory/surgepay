import { SagaStatus } from '../../generated/client';

export class SagaInstanceEntity {
  constructor(
    public readonly id: string,
    public readonly paymentId: string,
    public readonly correlationId: string,
    public status: SagaStatus,
    public version: number,
    public readonly startedAt: Date,
    public completedAt: Date | null,
    public readonly createdAt: Date,
    public readonly updatedAt: Date,
  ) {
    if (id !== correlationId) {
      throw new Error(
        `Invariant violation: Saga ID (id: ${id}) and Correlation ID (correlationId: ${correlationId}) must have identical values.`
      );
    }
  }

  /**
   * Static factory to initialize a new Saga aggregate root.
   * By doc-v3 Section 8.4, Saga ID (id) adopts the Correlation ID value exactly.
   */
  static create(params: {
    paymentId: string;
    correlationId: string;
  }): SagaInstanceEntity {
    return new SagaInstanceEntity(
      params.correlationId, // id adopts correlationId value
      params.paymentId,
      params.correlationId,
      SagaStatus.LEDGER_PENDING,
      0, // version starts at 0
      new Date(),
      null, // completedAt is populated only when CLOSED
      new Date(),
      new Date(),
    );
  }

  /**
   * Determines if the saga has reached a terminal state.
   */
  isTerminal(): boolean {
    return this.status === SagaStatus.CLOSED;
  }

  /**
   * Encapsulates state machine transitions.
   * Rejects invalid transitions and updates completedAt when transitioning to CLOSED.
   */
  transitionTo(nextState: SagaStatus): void {
    if (this.isTerminal()) {
      throw new Error(`Cannot transition from terminal state ${this.status} to ${nextState}`);
    }

    if (this.status === nextState) {
      return;
    }

    const isValid = this.isValidTransition(this.status, nextState);
    if (!isValid) {
      throw new Error(`Invalid saga state transition from ${this.status} to ${nextState}`);
    }

    this.status = nextState;

    if (nextState === SagaStatus.CLOSED) {
      this.completedAt = new Date();
    }
  }

  /**
   * Validates whether a transition is allowed under doc-v3 state machine rules.
   */
  private isValidTransition(current: SagaStatus, next: SagaStatus): boolean {
    // Normal forward flow:
    // LEDGER_PENDING -> LEDGER_RECORDED -> ELIGIBILITY_PENDING -> BALANCE_PENDING -> BALANCE_RESERVED -> NOTIFICATION_PENDING -> NOTIFIED -> CLOSED
    const forwardChain: SagaStatus[] = [
      SagaStatus.LEDGER_PENDING,
      SagaStatus.LEDGER_RECORDED,
      SagaStatus.ELIGIBILITY_PENDING,
      SagaStatus.BALANCE_PENDING,
      SagaStatus.BALANCE_RESERVED,
      SagaStatus.NOTIFICATION_PENDING,
      SagaStatus.NOTIFIED,
      SagaStatus.CLOSED,
    ];

    const currentIndex = forwardChain.indexOf(current);
    const nextIndex = forwardChain.indexOf(next);

    // If both are within the linear forward path, enforce nextIndex = currentIndex + 1
    if (currentIndex !== -1 && nextIndex !== -1) {
      if (nextIndex === currentIndex + 1) {
        return true;
      }
    }

    // Compensation/failure flows:
    // Any non-terminal state in the forward chain can transition to REVERSED upon failure (triggering compensation).
    // Note: entering REVERSED represents that all compensation operations (ledger reversal, balance reversal) have finished.
    if (next === SagaStatus.REVERSED) {
      return current !== SagaStatus.CLOSED && current !== SagaStatus.REVERSED;
    }

    // From REVERSED, we transition to CLOSED (terminal)
    if (current === SagaStatus.REVERSED && next === SagaStatus.CLOSED) {
      return true;
    }

    return false;
  }
}
