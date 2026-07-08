import { registerAs } from '@nestjs/config';

export const fraudConfig = registerAs('fraud', () => ({
  port: parseInt(process.env.PORT || '3008', 10),
  
  // Deterministic threshold for max amount allowed (e.g. 100,000.00 units in cents)
  maxAmountThreshold: parseInt(process.env.FRAUD_MAX_AMOUNT_THRESHOLD || '10000000', 10),
  
  // Comma-separated list of blacklisted merchant UUIDs
  blacklistedMerchants: (process.env.FRAUD_BLACKLISTED_MERCHANTS || '')
    .split(',')
    .map((id) => id.trim())
    .filter(Boolean),
    
  // Configuration-driven supported currencies list.
  // NOTE: This is a temporary implementation until a centralized configuration registry
  // is introduced across all services.
  supportedCurrencies: (process.env.FRAUD_SUPPORTED_CURRENCIES || 'INR,USD,EUR,GBP')
    .split(',')
    .map((c) => c.trim().toUpperCase())
    .filter(Boolean),
}));
