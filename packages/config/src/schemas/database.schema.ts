import { z } from 'zod';

export const databaseSchema = z.object({
  DATABASE_URL: z.string().url('DATABASE_URL must be a valid PostgreSQL connection URL'),
  DATABASE_HOST: z.string().optional(),
  DATABASE_PORT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().optional(),
  ),
  DATABASE_USER: z.string().optional(),
  DATABASE_PASSWORD: z.string().optional(),
  DATABASE_NAME: z.string().optional(),
  DATABASE_SCHEMA: z.string().optional(),
  DATABASE_SSL: z.preprocess((val) => val === 'true', z.boolean().default(false)),
  DATABASE_POOL_SIZE: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(10),
  ),
  DATABASE_CONNECT_TIMEOUT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(5000),
  ),
  DATABASE_IDLE_TIMEOUT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(30000),
  ),
});
