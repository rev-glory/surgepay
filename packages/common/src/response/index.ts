import type { ApiErrorResponse, SuccessResponse, ValidationErrorDetail } from '@surgepay/contracts';

export function createSuccessResponse<T>(data: T): SuccessResponse<T> {
  return {
    success: true,
    data,
  };
}

export function createErrorResponse(
  code: string,
  message: string,
  status: number,
  path: string,
  correlationId?: string,
  requestId?: string,
  validationErrors?: ValidationErrorDetail[],
  metadata?: Record<string, unknown>,
): ApiErrorResponse {
  return {
    success: false,
    error: {
      code,
      message,
      status,
      timestamp: new Date().toISOString(),
      path,
      correlationId,
      requestId,
      validationErrors,
      metadata,
    },
  };
}
