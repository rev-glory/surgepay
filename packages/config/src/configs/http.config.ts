import { registerAs } from '@nestjs/config';

import type { HttpConfig } from '../types';

export default registerAs('http', (): HttpConfig => ({
  port: process.env.PORT ? parseInt(process.env.PORT, 10) : 3000,
  host: process.env.HOST || 'localhost',
  apiPrefix: process.env.API_PREFIX || 'api/v1',
  keepAlive: process.env.HTTP_KEEP_ALIVE === 'true' || process.env.HTTP_KEEP_ALIVE === undefined,
  maxSockets: process.env.HTTP_KEEP_ALIVE_MAX_SOCKETS ? parseInt(process.env.HTTP_KEEP_ALIVE_MAX_SOCKETS, 10) : 100,
}));
