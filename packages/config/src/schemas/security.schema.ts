import { z } from 'zod';

export const securitySchema = z.object({
  JWT_SECRET: z.string().min(1, 'JWT_SECRET must be at least 1 character long'),
  API_KEY_HEADER: z.string().default('x-api-key'),
  CORS_ENABLED: z.preprocess((val) => val !== 'false', z.boolean().default(true)),
});
