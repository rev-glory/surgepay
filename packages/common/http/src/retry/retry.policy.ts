import axios from 'axios';

import type { LoggerService } from '@surgepay/common';

export interface RetryOptions {
  method: string;
  forceRetry?: boolean;
}

export class RetryPolicy {
  constructor(
    private readonly maxAttempts: number,
    private readonly delayMs: number,
    private readonly logger: LoggerService,
  ) {}

  async execute<T>(fn: () => Promise<T>, options: RetryOptions): Promise<T> {
    const isIdempotent = ['GET', 'PUT', 'DELETE'].includes(options.method.toUpperCase());
    const shouldRetry = options.forceRetry || isIdempotent;

    let attempt = 0;
    while (true) {
      try {
        return await fn();
      } catch (error: unknown) {
        attempt++;

        let status: number | undefined;
        let errorMessage = String(error);

        if (axios.isAxiosError(error)) {
          status = error.response?.status;
          errorMessage = error.message;
        } else if (error instanceof Error) {
          errorMessage = error.message;
        }

        const is4xx = status !== undefined && status >= 400 && status < 500;

        if (!shouldRetry || is4xx || attempt > this.maxAttempts) {
          throw error;
        }

        const backoffDelay = this.delayMs * Math.pow(2, attempt - 1);
        this.logger.warn(
          `Request failed. Retrying (attempt ${attempt}/${this.maxAttempts}) in ${backoffDelay}ms... Error: ${errorMessage}`,
        );

        await new Promise((resolve) => setTimeout(resolve, backoffDelay));
      }
    }
  }
}
