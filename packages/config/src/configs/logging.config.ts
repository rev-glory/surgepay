import { registerAs } from '@nestjs/config';

import type { LoggingConfig } from '../types';

export default registerAs('logging', (): LoggingConfig => ({
  level: (process.env.LOG_LEVEL || 'info') as LoggingConfig['level'],
  pretty: process.env.LOG_PRETTY === 'true',
  serviceName: process.env.LOG_SERVICE_NAME || 'surgepay-service',
}));
