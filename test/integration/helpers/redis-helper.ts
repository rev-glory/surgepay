import { getRedisClient } from './test-setup';

/**
 * Wipes all rate limits and idempotency keys from Redis.
 */
export async function clearRedis(): Promise<void> {
  const redis = getRedisClient();
  const keys = await redis.keys('*');
  const targetKeys = keys.filter(
    (key) => key.startsWith('rate_limit:') || key.startsWith('idem:'),
  );

  if (targetKeys.length > 0) {
    await redis.del(...targetKeys);
  }
}

/**
 * Directly queries Redis to retrieve an idempotency record and returns it.
 */
export async function getIdempotencyRecord(
  merchantId: string,
  idempotencyKey: string,
): Promise<{ record: Record<string, unknown> | null; ttl: number }> {
  const redis = getRedisClient();
  const redisKey = `idem:${merchantId}:${idempotencyKey}`;
  const data = await redis.get(redisKey);
  const ttl = await redis.ttl(redisKey);

  if (!data) {
    return { record: null, ttl };
  }

  return {
    record: JSON.parse(data) as Record<string, unknown>,
    ttl,
  };
}
