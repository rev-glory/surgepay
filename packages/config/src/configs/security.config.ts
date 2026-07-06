import { registerAs } from '@nestjs/config';

import type { SecurityConfig } from '../types';

export default registerAs('security', (): SecurityConfig => ({
  jwtSecret: process.env.JWT_SECRET || '',
  apiKeyHeader: process.env.API_KEY_HEADER || 'x-api-key',
  corsEnabled: process.env.CORS_ENABLED !== 'false',
}));
