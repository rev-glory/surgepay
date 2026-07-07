/**
 * Generates the unique Redis key for a given merchant and client-provided idempotency key.
 * Format: idem:{merchantId}:{idempotencyKey}
 */
export const getRedisKey = (merchantId: string, idempotencyKey: string): string => {
  return `idem:${merchantId}:${idempotencyKey}`;
};
