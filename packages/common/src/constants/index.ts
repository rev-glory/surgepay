export const HEADERS = {
  CORRELATION_ID: 'x-correlation-id',
  CAUSATION_ID: 'x-causation-id',
  SAGA_ID: 'x-saga-id',
  IDEMPOTENCY_KEY: 'x-idempotency-key',
  REQUEST_ID: 'x-request-id',
} as const;

export const EVENT_VERSION = 1;
export const DEFAULT_TIMEZONE = 'UTC';
