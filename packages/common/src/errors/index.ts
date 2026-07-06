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
  readonly code = 'VALIDATION_ERROR';
}

export class NotFoundError extends BaseError {
  readonly statusCode = 404;
  readonly code = 'NOT_FOUND_ERROR';
}

export class ConflictError extends BaseError {
  readonly statusCode = 409;
  readonly code = 'CONFLICT_ERROR';
}

export class UnauthorizedError extends BaseError {
  readonly statusCode = 401;
  readonly code = 'UNAUTHORIZED_ERROR';
}

export class ForbiddenError extends BaseError {
  readonly statusCode = 403;
  readonly code = 'FORBIDDEN_ERROR';
}

export class InternalServerError extends BaseError {
  readonly statusCode = 500;
  readonly code = 'INTERNAL_SERVER_ERROR';
}
