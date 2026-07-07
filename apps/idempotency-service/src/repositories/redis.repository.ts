import { Inject, Injectable } from '@nestjs/common';
import Redis from 'ioredis';

import { getRedisKey } from '../constants/redis-keys';
import { RequestStatus } from '../constants/request-status';
import { IdempotencyRecord } from '../interfaces/idempotency-record.interface';

@Injectable()
export class RedisRepository {
  constructor(@Inject('REDIS_CLIENT') private readonly redis: Redis) {}

  /**
   * Finds an existing idempotency record by key.
   */
  async findRequest(merchantId: string, idempotencyKey: string): Promise<IdempotencyRecord | null> {
    const key = getRedisKey(merchantId, idempotencyKey);
    const data = await this.redis.get(key);
    if (!data) return null;
    return JSON.parse(data) as IdempotencyRecord;
  }

  /**
   * Atomically creates a request in the IN_PROGRESS state.
   * Returns true if the key was created, or false if it already exists.
   */
  async createInProgress(
    merchantId: string,
    idempotencyKey: string,
    requestHash: string,
    ownerId: string,
    ttlHours: number,
  ): Promise<boolean> {
    const key = getRedisKey(merchantId, idempotencyKey);
    const ttlSeconds = ttlHours * 3600;
    const now = new Date();
    const expiresAt = new Date(now.getTime() + ttlSeconds * 1000);

    const record: IdempotencyRecord = {
      status: RequestStatus.IN_PROGRESS,
      ownerId,
      requestHash,
      statusCode: null,
      headers: null,
      body: null,
      createdAt: now.toISOString(),
      expiresAt: expiresAt.toISOString(),
    };

    const serialized = JSON.stringify(record);

    // Atomic SET if Not Exists with Expiry
    const result = await this.redis.set(key, serialized, 'EX', ttlSeconds, 'NX');
    return result === 'OK';
  }

  /**
   * Transitions request state to COMPLETED atomically if the ownerId matches.
   * Returns true if successfully updated, false otherwise.
   */
  async markCompleted(
    merchantId: string,
    idempotencyKey: string,
    ownerId: string,
    requestHash: string,
    statusCode: number,
    headers: Record<string, string>,
    body: Record<string, unknown>,
    ttlHours: number,
  ): Promise<boolean> {
    const key = getRedisKey(merchantId, idempotencyKey);
    const ttlSeconds = ttlHours * 3600;
    const now = new Date();
    const expiresAt = new Date(now.getTime() + ttlSeconds * 1000);

    const record: IdempotencyRecord = {
      status: RequestStatus.COMPLETED,
      ownerId,
      requestHash,
      statusCode,
      headers,
      body,
      createdAt: now.toISOString(), // Preserve or update. Here we track when it was completed.
      expiresAt: expiresAt.toISOString(),
    };

    const serialized = JSON.stringify(record);

    // Lua script to atomically compare ownerId and set completed record
    const script = `
      local existing = redis.call('GET', KEYS[1])
      if not existing then
        return 0
      end
      local data = cjson.decode(existing)
      if data.ownerId == ARGV[1] then
        redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
        return 1
      else
        return 0
      end
    `;

    const result = await this.redis.eval(script, 1, key, ownerId, serialized, String(ttlSeconds));
    return result === 1;
  }

  /**
   * Safely deletes an in-progress record (releases the lock) if the ownerId matches.
   * Returns true if deleted, false otherwise.
   */
  async delete(merchantId: string, idempotencyKey: string, ownerId: string): Promise<boolean> {
    const key = getRedisKey(merchantId, idempotencyKey);

    const script = `
      local existing = redis.call('GET', KEYS[1])
      if not existing then
        return 0
      end
      local data = cjson.decode(existing)
      if data.ownerId == ARGV[1] then
        redis.call('DEL', KEYS[1])
        return 1
      else
        return 0
      end
    `;

    const result = await this.redis.eval(script, 1, key, ownerId);
    return result === 1;
  }

  /**
   * Extends the TTL of a key if it exists.
   */
  async extendTTL(merchantId: string, idempotencyKey: string, ttlHours: number): Promise<boolean> {
    const key = getRedisKey(merchantId, idempotencyKey);
    const result = await this.redis.expire(key, ttlHours * 3600);
    return result === 1;
  }

  /**
   * Checks if a key exists in Redis.
   */
  async exists(merchantId: string, idempotencyKey: string): Promise<boolean> {
    const key = getRedisKey(merchantId, idempotencyKey);
    const count = await this.redis.exists(key);
    return count > 0;
  }
}
