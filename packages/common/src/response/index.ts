export interface SuccessResponse<T> {
  success: true;
  data: T;
  meta?: unknown;
}

export interface ErrorResponse {
  success: false;
  error: {
    code: string;
    message: string;
    details?: unknown;
  };
}

export function createSuccessResponse<T>(data: T, meta?: unknown): SuccessResponse<T> {
  return {
    success: true,
    data,
    meta,
  };
}

export function createErrorResponse(
  code: string,
  message: string,
  details?: unknown,
): ErrorResponse {
  return {
    success: false,
    error: {
      code,
      message,
      details,
    },
  };
}
