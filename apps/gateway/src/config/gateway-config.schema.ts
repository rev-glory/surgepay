import { z } from 'zod';

export const gatewayConfigSchema = z.object({
  MERCHANT_SERVICE_URL: z.string().url('MERCHANT_SERVICE_URL must be a valid URL'),
  MERCHANT_SERVICE_TIMEOUT: z.preprocess(
    (val: unknown) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(5000),
  ),
  IDEMPOTENCY_SERVICE_URL: z.string().url('IDEMPOTENCY_SERVICE_URL must be a valid URL'),
  IDEMPOTENCY_SERVICE_TIMEOUT: z.preprocess(
    (val: unknown) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(5000),
  ),
  RATE_LIMIT_WINDOW_SECONDS: z.preprocess(
    (val: unknown) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(60),
  ),
  RATE_LIMIT_DEFAULT_LIMIT: z.preprocess(
    (val: unknown) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(100),
  ),
});


export type GatewayConfig = z.infer<typeof gatewayConfigSchema>;

export function getGatewayConfig(): GatewayConfig {
  const parsed = gatewayConfigSchema.safeParse(process.env);
  if (!parsed.success) {
    // eslint-disable-next-line no-console
    console.error('\n❌ Gateway environment validation failed:');
    parsed.error.errors.forEach((err: z.ZodIssue) => {
      // eslint-disable-next-line no-console
      console.error(`   - [${err.path.join('.')}] ${err.message}`);
    });
    throw new Error('Gateway environment validation failed');
  }
  return parsed.data;
}

