export enum PlatformErrorCode {
  INVALID_API_KEY = 'INVALID_API_KEY',
  MERCHANT_DISABLED = 'MERCHANT_DISABLED',
  RATE_LIMIT_EXCEEDED = 'RATE_LIMIT_EXCEEDED',
  MISSING_IDEMPOTENCY_KEY = 'MISSING_IDEMPOTENCY_KEY',
  IDEMPOTENCY_CONFLICT = 'IDEMPOTENCY_CONFLICT',
  INVALID_REQUEST = 'INVALID_REQUEST',
  VALIDATION_FAILED = 'VALIDATION_FAILED',
  INTERNAL_ERROR = 'INTERNAL_ERROR',
  PAYMENT_BLOCKED = 'PAYMENT_BLOCKED',
}

export interface ValidationErrorDetail {
  field: string;
  rejectedValue: unknown;
  rule: string;
  message: string;
}

export interface ApiErrorDetail {
  code: string;
  message: string;
  status: number;
  timestamp: string;
  path: string;
  correlationId?: string;
  requestId?: string;
  validationErrors?: ValidationErrorDetail[];
  metadata?: Record<string, unknown>;
}

export interface ApiErrorResponse {
  success: false;
  error: ApiErrorDetail;
}
