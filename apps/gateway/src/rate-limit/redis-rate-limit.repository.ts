import { Inject, Injectable, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';

@Injectable()
export class RedisRateLimitRepository implements OnModuleDestroy {
  // Lua script to atomically implement sliding window rate limiting.
  // 1. Cleans elements older than now - window.
  // 2. Counts remaining requests.
  // 3. If count < limit, adds the request and updates key TTL.
  // 4. Returns [allowed, remaining_quota, reset_time_ms].
  private readonly luaScript = `
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])
    local request_id = ARGV[4]

    local clear_before = now - window
    redis.call('zremrangebyscore', key, '-inf', clear_before)

    local amount = redis.call('zcard', key)
    local allowed = 0

    if amount < limit then
      redis.call('zadd', key, now, request_id)
      redis.call('pexpire', key, window)
      allowed = 1
      amount = amount + 1
    end

    -- Determine the reset time when a client will be allowed to execute again.
    -- This is either when the oldest element exits the window, or if set is empty, now + window.
    local oldest = redis.call('zrange', key, 0, 0, 'WITHSCORES')
    local reset_time = now + window
    if #oldest > 0 then
      reset_time = tonumber(oldest[2]) + window
    end

    return { allowed, limit - amount, reset_time }
  `;

  constructor(@Inject('REDIS_CLIENT') private readonly redisClient: Redis) {}

  async onModuleDestroy(): Promise<void> {
    try {
      await this.redisClient.quit();
    } catch {
      this.redisClient.disconnect();
    }
  }

  /**
   * Check sliding window rate limit atomically.
   *
   * @param merchantId Unique identifier for the merchant
   * @param limit Quota allowed within the window
   * @param windowSeconds Duration of sliding window in seconds
   * @param requestId Unique identifier for request tracing and uniqueness in ZSET
   */
  async checkRateLimit(
    merchantId: string,
    limit: number,
    windowSeconds: number,
    requestId: string,
  ): Promise<{ allowed: boolean; remaining: number; resetTimeMs: number }> {
    const key = `rate_limit:${merchantId}`;
    const now = Date.now(); // milliseconds epoch
    const windowMs = windowSeconds * 1000;

    // Execute eval to run Lua script atomically
    const result = (await this.redisClient.eval(
      this.luaScript,
      1,
      key,
      now.toString(),
      windowMs.toString(),
      limit.toString(),
      requestId,
    )) as [number, number, number];

    const [allowed, remaining, resetTimeMs] = result;

    return {
      allowed: allowed === 1,
      remaining,
      resetTimeMs,
    };
  }
}
