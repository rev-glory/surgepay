import { z } from 'zod';

export const loggingSchema = z.object({
  LOG_LEVEL: z.enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal']).default('info'),
  LOG_PRETTY: z.preprocess(
    (val) => val === 'true',
    z.boolean().default(false)
  ),
  LOG_SERVICE_NAME: z.string().min(1, 'LOG_SERVICE_NAME is required'),
});
