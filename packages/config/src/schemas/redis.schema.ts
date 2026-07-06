import { z } from 'zod';

export const redisSchema = z.object({
  REDIS_URL: z.string().url('REDIS_URL must be a valid Redis connection URL'),
  REDIS_PASSWORD: z.string().optional(),
  REDIS_TLS: z.preprocess(
    (val) => val === 'true',
    z.boolean().default(false)
  ),
});
