export interface RetryPolicy {
  shouldMoveToDlq(retryCount: number): boolean;
}

export class MaxAttemptsRetryPolicy implements RetryPolicy {
  constructor(private readonly maxRetries: number) {}

  shouldMoveToDlq(retryCount: number): boolean {
    return retryCount >= this.maxRetries;
  }
}
