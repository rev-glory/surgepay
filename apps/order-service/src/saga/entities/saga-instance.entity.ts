import { OrderValidationStatus, SagaStatus } from '../../generated/client';

export class SagaInstanceEntity {
  constructor(
    public readonly id: string,
    public readonly paymentId: string,
    public readonly correlationId: string,
    public status: SagaStatus,
    public orderValidationStatus: OrderValidationStatus,
    public readonly merchantId: string,
    public readonly amount: number,
    public readonly currency: string,
    public version: number,
    public readonly startedAt: Date,
    public completedAt: Date | null,
    public readonly createdAt: Date,
    public readonly updatedAt: Date,
    public failureReason: string | null = null,
    public failedAt: Date | null = null,
    public originService: string | null = null,
    public stateUpdatedAt: Date = new Date(),
    public retryCount: number = 0,
    public lastRetryAt: Date | null = null,
    public nextRetryAt: Date | null = null,
    public currentCommandId: string | null = null,
    public retryHandoffAt: Date | null = null,
    public recoveredAt: Date | null = null,
    public recoveryCount: number = 0,
    public recoveryReason: string | null = null
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
    merchantId: string;
    amount: number;
    currency: string;
    initialCommandId?: string;
  }): SagaInstanceEntity {
    const now = new Date();
    return new SagaInstanceEntity(
      params.correlationId, // id adopts correlationId value
      params.paymentId,
      params.correlationId,
      SagaStatus.LEDGER_PENDING,
      OrderValidationStatus.PENDING,
      params.merchantId,
      params.amount,
      params.currency,
      0, // version starts at 0
      now,
      null, // completedAt is populated only when CLOSED
      now,
      now,
      null,
      null,
      null,
      now, // stateUpdatedAt
      0,   // retryCount
      null, // lastRetryAt
      null, // nextRetryAt
      params.initialCommandId || null, // currentCommandId
      null, // retryHandoffAt
      null, // recoveredAt
      0,    // recoveryCount
      null  // recoveryReason
    );
  }

  /**
   * Determines if the saga has reached a terminal state.
   */
  isTerminal(): boolean {
    return this.status === SagaStatus.CLOSED;
  }

  /**
   * Determines if order validation has reached a terminal state.
   */
  isOrderValidationTerminal(): boolean {
    return (
      this.orderValidationStatus === OrderValidationStatus.CONFIRMED ||
      this.orderValidationStatus === OrderValidationStatus.REJECTED
    );
  }

  /**
   * Checks if the saga is allowed to proceed forward.
   */
  canProceedForward(): boolean {
    return this.orderValidationStatus !== OrderValidationStatus.REJECTED && this.failureReason === null;
  }

  /**
   * Confirms order eligibility.
   */
  confirmOrder(): void {
    if (this.isTerminal()) {
      throw new Error('Cannot confirm order validation on a terminal saga.');
    }
    if (this.orderValidationStatus !== OrderValidationStatus.PENDING) {
      throw new Error(
        `Invalid order validation transition from ${this.orderValidationStatus} to CONFIRMED`
      );
    }
    this.orderValidationStatus = OrderValidationStatus.CONFIRMED;
  }

  /**
   * Rejects order eligibility and saves failure details.
   */
  rejectOrder(reason: string, service: string): void {
    if (this.isTerminal()) {
      throw new Error('Cannot reject order validation on a terminal saga.');
    }
    if (this.orderValidationStatus !== OrderValidationStatus.PENDING) {
      throw new Error(
        `Invalid order validation transition from ${this.orderValidationStatus} to REJECTED`
      );
    }
    this.orderValidationStatus = OrderValidationStatus.REJECTED;
    this.failureReason = reason;
    this.failedAt = new Date();
    this.originService = service;
  }

  /**
   * Starts a retry handoff to the Retry Scheduler.
   * Atomic marker to prevent scanner races.
   */
  startHandoff(): void {
    if (this.isTerminal() || !this.canProceedForward()) {
      throw new Error('Cannot start retry handoff on a terminal or failed saga.');
    }
    this.retryHandoffAt = new Date();
  }

  /**
   * Registers a scheduled retry from the Retry Scheduler.
   * Clears handoff status and syncs timer details.
   */
  registerRetry(attempt: number, nextExecutionTime: Date): void {
    this.retryHandoffAt = null;
    this.retryCount = attempt;
    this.nextRetryAt = nextExecutionTime;
    this.lastRetryAt = new Date();
  }

  /**
   * Transition the saga to a step-failure state upon retry exhaustion.
   * Clears handoff status and timing metadata.
   */
  failStep(reason: string, service: string): void {
    this.retryHandoffAt = null;
    this.nextRetryAt = null;
    this.failureReason = reason;
    this.failedAt = new Date();
    this.originService = service;
  }

  /**
   * Reset retry metadata when a business step successfully advances.
   */
  resetRetryMetadata(nextCommandId: string | null = null): void {
    this.retryCount = 0;
    this.nextRetryAt = null;
    this.retryHandoffAt = null;
    this.lastRetryAt = null;
    this.currentCommandId = nextCommandId;
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

    // Invariant: cannot proceed to ledger recording unless order validation is confirmed
    if (
      nextState === SagaStatus.LEDGER_RECORDED &&
      this.orderValidationStatus !== OrderValidationStatus.CONFIRMED
    ) {
      throw new Error(
        `Cannot transition financial status to LEDGER_RECORDED when order validation is ${this.orderValidationStatus}`
      );
    }

    // Invariant: block forward financial execution when order validation is rejected or saga has failed
    if (
      !this.canProceedForward() &&
      nextState !== SagaStatus.REVERSED &&
      nextState !== SagaStatus.CLOSED
    ) {
      throw new Error(
        `Cannot perform forward transition to ${nextState} when order validation is REJECTED or Saga has failed`
      );
    }

    const isValid = this.isValidTransition(this.status, nextState);
    if (!isValid) {
      throw new Error(`Invalid saga state transition from ${this.status} to ${nextState}`);
    }

    this.status = nextState;
    this.stateUpdatedAt = new Date();

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

  recordRecovery(reason: string): void {
    this.recoveredAt = new Date();
    this.recoveryCount += 1;
    this.recoveryReason = reason;
  }
}
