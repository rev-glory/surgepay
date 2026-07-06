import { registerAs } from '@nestjs/config';

import type { RedisConfig } from '../types';

export default registerAs('redis', (): RedisConfig => ({
  url: process.env.REDIS_URL!,
  password: process.env.REDIS_PASSWORD,
  tls: process.env.REDIS_TLS === 'true',
}));
