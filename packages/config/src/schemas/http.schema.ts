import { z } from 'zod';

export const httpSchema = z.object({
  PORT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(3000),
  ),
  HOST: z.string().default('localhost'),
  API_PREFIX: z.string().default('api/v1'),
  HTTP_KEEP_ALIVE: z.preprocess(
    (val) => (val === undefined ? undefined : val === 'true'),
    z.boolean().optional(),
  ),
  HTTP_KEEP_ALIVE_MAX_SOCKETS: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().optional(),
  ),
});
