import { z } from 'zod';

export const servicesSchema = z.object({
  GATEWAY_URL: z.string().url().default('http://localhost:3000'),
  MERCHANT_SERVICE_URL: z.string().url().default('http://localhost:3001'),
  PAYMENT_SERVICE_URL: z.string().url().default('http://localhost:3003'),
  ORDER_SERVICE_URL: z.string().url().default('http://localhost:3004'),
  LEDGER_SERVICE_URL: z.string().url().default('http://localhost:3005'),
  BALANCE_SERVICE_URL: z.string().url().default('http://localhost:3006'),
  NOTIFICATION_SERVICE_URL: z.string().url().default('http://localhost:3007'),
  INTERNAL_REQUEST_TIMEOUT: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().positive().default(2000),
  ),
  INTERNAL_REQUEST_RETRIES: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().nonnegative().default(3),
  ),
  INTERNAL_REQUEST_RETRY_DELAY: z.preprocess(
    (val) => (val ? parseInt(String(val), 10) : undefined),
    z.number().int().nonnegative().default(100),
  ),
});
