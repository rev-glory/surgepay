import * as crypto from 'crypto';
import { getPrismaClient } from './test-setup';

/**
 * Utility to hash API keys using SHA-256 (matching the merchant-service's internal hashing design).
 */
export function hashApiKey(apiKey: string): string {
  return crypto.createHash('sha256').update(apiKey).digest('hex');
}

/**
 * Wipes the database tables related to merchants and API keys.
 */
export async function clearDatabase(): Promise<void> {
  const prisma = getPrismaClient();
  // Cascade delete or delete in correct dependency order (ApiKey depends on Merchant)
  await prisma.merchantApiKey.deleteMany({});
  await prisma.merchant.deleteMany({});
}

/**
 * Seeds a test merchant and associated API key.
 */
export async function createTestMerchant(options: {
  apiKey: string;
  name: string;
  email: string;
  status?: string;
  permissions?: string[];
  webhookEnabled?: boolean;
}): Promise<{ id: string; merchantId: string }> {
  const prisma = getPrismaClient();

  const merchant = await prisma.merchant.create({
    data: {
      name: options.name,
      email: options.email,
      status: options.status ?? 'ACTIVE',
      metadata: {
        permissions: options.permissions ?? [],
        webhookEnabled: options.webhookEnabled ?? true,
      },
    },
  });

  await prisma.merchantApiKey.create({
    data: {
      apiKeyHash: hashApiKey(options.apiKey),
      merchantId: merchant.id,
      active: true,
    },
  });

  return {
    id: merchant.id,
    merchantId: merchant.merchantId,
  };
}

/**
 * Creates a revoked api key for a test merchant.
 */
export async function createRevokedApiKey(options: {
  apiKey: string;
  name: string;
  email: string;
}): Promise<void> {
  const prisma = getPrismaClient();

  const merchant = await prisma.merchant.create({
    data: {
      name: options.name,
      email: options.email,
      status: 'ACTIVE',
      metadata: {
        permissions: ['payment:create'],
        webhookEnabled: true,
      },
    },
  });

  await prisma.merchantApiKey.create({
    data: {
      apiKeyHash: hashApiKey(options.apiKey),
      merchantId: merchant.id,
      active: false,
      revokedAt: new Date(),
    },
  });
}
