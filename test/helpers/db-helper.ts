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
  try {
    await prisma.$executeRawUnsafe('TRUNCATE TABLE "payment"."Payment" CASCADE;');
  } catch (err) {
    // Ignore if table/schema is not pushed yet in other contexts
  }
  try {
    await prisma.$executeRawUnsafe('TRUNCATE TABLE "payment"."OutboxEvent" CASCADE;');
  } catch (err) {
    // Ignore if table/schema is not pushed yet in other contexts
  }
  try {
    await prisma.$executeRawUnsafe('TRUNCATE TABLE "order"."Order" CASCADE;');
  } catch (err) {
    // Ignore if table/schema is not pushed yet in other contexts
  }
  try {
    await prisma.$executeRawUnsafe('TRUNCATE TABLE "order"."InboxEvent" CASCADE;');
  } catch (err) {
    // Ignore if table/schema is not pushed yet in other contexts
  }
}

/**
 * Seeding a test merchant and associated API key.
 */
export async function createTestMerchant(options: {
  apiKey: string;
  name: string;
  email: string;
  status?: string;
  permissions?: string[];
  webhookEnabled?: boolean;
  merchantId?: string;
}): Promise<{ id: string; merchantId: string }> {
  const prisma = getPrismaClient();

  const merchant = await prisma.merchant.create({
    data: {
      merchantId: options.merchantId,
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

/**
 * Seeding helper for test orders consistent with Payment Service test setup.
 */
export async function createTestOrder(options: {
  merchantId: string;
  reference: string;
  amount: number;
  currency: string;
  status: string;
}): Promise<{ id: string }> {
  const prisma = getPrismaClient();
  const results = await prisma.$queryRawUnsafe<{ id: string }[]>(
    `INSERT INTO "order"."Order" (id, "merchantId", amount, currency, status, reference, "createdAt", "updatedAt")
     VALUES (gen_random_uuid(), $1::uuid, $2, $3, $4::"order"."OrderStatus", $5, NOW(), NOW())
     RETURNING id;`,
    options.merchantId,
    options.amount,
    options.currency,
    options.status,
    options.reference,
  );
  const firstResult = results[0];
  if (!firstResult) {
    throw new Error('Failed to create test order in database.');
  }
  return firstResult;
}

/**
 * Seeding / verification helper to count payment records in the database.
 */
export async function getPaymentCount(merchantId: string, reference: string): Promise<number> {
  const prisma = getPrismaClient();
  const result = await prisma.$queryRawUnsafe<{ count: any }[]>(
    `SELECT COUNT(*) as count FROM "payment"."Payment" WHERE "merchantId" = $1::uuid AND reference = $2;`,
    merchantId,
    reference,
  );
  return Number(result[0]?.count ?? 0);
}

/**
 * Seeding / verification helper to count outbox records in the database.
 */
export async function getOutboxCount(aggregateId: string): Promise<number> {
  const prisma = getPrismaClient();
  const result = await prisma.$queryRawUnsafe<{ count: any }[]>(
    `SELECT COUNT(*) as count FROM "payment"."OutboxEvent" WHERE "aggregateId" = $1::uuid;`,
    aggregateId,
  );
  return Number(result[0]?.count ?? 0);
}

export async function getOutboxEvents(aggregateId: string): Promise<any[]> {
  const prisma = getPrismaClient();
  const result = await prisma.$queryRawUnsafe<any[]>(
    `SELECT * FROM "payment"."OutboxEvent" WHERE "aggregateId" = $1::uuid;`,
    aggregateId,
  );
  return result;
}

/**
 * Seeding / verification helper to get payment records in the database.
 */
export async function getPaymentRecords(merchantId: string, reference: string): Promise<any[]> {
  const prisma = getPrismaClient();
  const result = await prisma.$queryRawUnsafe<any[]>(
    `SELECT * FROM "payment"."Payment" WHERE "merchantId" = $1::uuid AND reference = $2;`,
    merchantId,
    reference,
  );
  return result;
}
