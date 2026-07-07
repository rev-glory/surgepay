import { registerAs } from '@nestjs/config';

import type { ServicesConfig } from '../types';

export default registerAs('services', (): ServicesConfig => ({
  gatewayUrl: process.env.GATEWAY_URL || 'http://localhost:3000',
  merchantServiceUrl: process.env.MERCHANT_SERVICE_URL || 'http://localhost:3001',
  paymentServiceUrl: process.env.PAYMENT_SERVICE_URL || 'http://localhost:3003',
  orderServiceUrl: process.env.ORDER_SERVICE_URL || 'http://localhost:3004',
  ledgerServiceUrl: process.env.LEDGER_SERVICE_URL || 'http://localhost:3005',
  balanceServiceUrl: process.env.BALANCE_SERVICE_URL || 'http://localhost:3006',
  notificationServiceUrl: process.env.NOTIFICATION_SERVICE_URL || 'http://localhost:3007',
  internalRequestTimeout: process.env.INTERNAL_REQUEST_TIMEOUT
    ? parseInt(process.env.INTERNAL_REQUEST_TIMEOUT, 10)
    : 2000,
  internalRequestRetries: process.env.INTERNAL_REQUEST_RETRIES
    ? parseInt(process.env.INTERNAL_REQUEST_RETRIES, 10)
    : 3,
  internalRequestRetryDelay: process.env.INTERNAL_REQUEST_RETRY_DELAY
    ? parseInt(process.env.INTERNAL_REQUEST_RETRY_DELAY, 10)
    : 100,
}));
