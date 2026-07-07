import { registerAs } from '@nestjs/config';

import type { DatabaseConfig } from '../types';

export default registerAs('database', (): DatabaseConfig => ({
  url: process.env.DATABASE_URL!,
  host: process.env.DATABASE_HOST,
  port: process.env.DATABASE_PORT ? parseInt(process.env.DATABASE_PORT, 10) : undefined,
  username: process.env.DATABASE_USER,
  password: process.env.DATABASE_PASSWORD,
  database: process.env.DATABASE_NAME,
  schema: process.env.DATABASE_SCHEMA,
  ssl: process.env.DATABASE_SSL === 'true',
  poolSize: process.env.DATABASE_POOL_SIZE ? parseInt(process.env.DATABASE_POOL_SIZE, 10) : 10,
  connectTimeout: process.env.DATABASE_CONNECT_TIMEOUT
    ? parseInt(process.env.DATABASE_CONNECT_TIMEOUT, 10)
    : 5000,
  idleTimeout: process.env.DATABASE_IDLE_TIMEOUT
    ? parseInt(process.env.DATABASE_IDLE_TIMEOUT, 10)
    : 30000,
}));
