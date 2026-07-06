import { z } from 'zod';

export const httpSchema = z.object({
  PORT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(3000)
  ),
  HOST: z.string().default('localhost'),
  API_PREFIX: z.string().default('api/v1'),
});
