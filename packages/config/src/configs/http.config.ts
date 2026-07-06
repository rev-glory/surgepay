import { registerAs } from '@nestjs/config';

import type { HttpConfig } from '../types';

export default registerAs('http', (): HttpConfig => ({
  port: process.env.PORT ? parseInt(process.env.PORT, 10) : 3000,
  host: process.env.HOST || 'localhost',
  apiPrefix: process.env.API_PREFIX || 'api/v1',
}));
