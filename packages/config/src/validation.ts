/* eslint-disable no-console */
import { z } from 'zod';

import { databaseSchema } from './schemas/database.schema';
import { httpSchema } from './schemas/http.schema';
import { kafkaSchema } from './schemas/kafka.schema';
import { loggingSchema } from './schemas/logging.schema';
import { redisSchema } from './schemas/redis.schema';
import { securitySchema } from './schemas/security.schema';
import { servicesSchema } from './schemas/services.schema';
import { telemetrySchema } from './schemas/telemetry.schema';
import { outboxSchema } from './schemas/outbox.schema';

export const environmentSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  })
  .merge(databaseSchema)
  .merge(redisSchema)
  .merge(kafkaSchema)
  .merge(httpSchema)
  .merge(loggingSchema)
  .merge(telemetrySchema)
  .merge(securitySchema)
  .merge(servicesSchema)
  .merge(outboxSchema);

export type Environment = z.infer<typeof environmentSchema>;

export function validate(config: Record<string, unknown>): Environment {
  const parsed = environmentSchema.safeParse(config);

  if (!parsed.success) {
    console.error('\n❌ Environment validation failed. Please check your environment variables:');
    parsed.error.errors.forEach((err) => {
      console.error(`   - [${err.path.join('.')}] ${err.message}`);
    });
    console.error('');
    throw new Error('Environment validation failed');
  }

  return parsed.data;
}
