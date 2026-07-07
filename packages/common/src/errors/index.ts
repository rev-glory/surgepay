import { PlatformErrorCode } from '@surgepay/contracts';

export abstract class BaseError extends Error {
  abstract readonly statusCode: number;
  abstract readonly code: string;

  constructor(
    message: string,
    public readonly details?: unknown,
  ) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor);
    }
  }
}

export class ValidationError extends BaseError {
  readonly statusCode = 400;
  readonly code = PlatformErrorCode.VALIDATION_FAILED;
}

export class NotFoundError extends BaseError {
  readonly statusCode = 404;
  readonly code = PlatformErrorCode.INVALID_REQUEST;
}

export class ConflictError extends BaseError {
  readonly statusCode = 409;
  readonly code = PlatformErrorCode.IDEMPOTENCY_CONFLICT;
}

export class UnauthorizedError extends BaseError {
  readonly statusCode = 401;
  readonly code = PlatformErrorCode.INVALID_API_KEY;
}

export class ForbiddenError extends BaseError {
  readonly statusCode = 403;
  readonly code = PlatformErrorCode.MERCHANT_DISABLED;
}

export class InternalServerError extends BaseError {
  readonly statusCode = 500;
  readonly code = PlatformErrorCode.INTERNAL_ERROR;
}
