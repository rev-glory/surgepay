import { BaseError } from './base.error';
import type { DomainErrorCode } from './error-codes';

export class DomainException<TMetadata extends Record<string, unknown> = Record<string, unknown>> extends BaseError {
  public override readonly cause?: Error;

  constructor(
    message: string,
    public override readonly code: DomainErrorCode,
    public override readonly statusCode: number,
    public readonly metadata?: TMetadata,
    options?: { cause?: Error },
  ) {
    super(message, metadata);
    if (options?.cause) {
      this.cause = options.cause;
    }
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
