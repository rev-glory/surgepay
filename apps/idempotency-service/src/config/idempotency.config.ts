import { registerAs } from '@nestjs/config';

export interface IdempotencyConfig {
  ttlHours: number;
  redisPrefix: string;
  conflictMode: number;
}

export default registerAs('idempotency', (): IdempotencyConfig => ({
  ttlHours: parseInt(process.env.IDEMPOTENCY_TTL_HOURS || '24', 10),
  redisPrefix: process.env.IDEMPOTENCY_REDIS_PREFIX || 'idem',
  conflictMode: parseInt(process.env.IDEMPOTENCY_CONFLICT_MODE || '409', 10),
}));
