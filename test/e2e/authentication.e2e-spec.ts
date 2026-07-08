import * as request from 'supertest';
import { INestApplication } from '@nestjs/common';
import * as crypto from 'crypto';

import { setupE2EEnvironment, teardownE2EEnvironment } from '../helpers/test-setup';
import { clearDatabase, createTestMerchant, createRevokedApiKey, createTestOrder } from '../helpers/db-helper';
import { clearRedis } from '../helpers/redis-helper';
import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';

describe('API Gateway - E2E Authentication Pipeline', () => {
  let app: INestApplication;

  beforeAll(async () => {
    const environment = await setupE2EEnvironment();
    app = environment.gatewayApp;
  });

  afterAll(async () => {
    await teardownE2EEnvironment();
  });

  beforeEach(async () => {
    await clearDatabase();
    await clearRedis();
  });

  it('should accept a request with a valid active merchant API key', async () => {
    const merchant = await createTestMerchant(MERCHANT_FIXTURES.active);
    const merchantId = merchant.merchantId;

    const idempotencyKey = `idem_auth_ok_${Date.now()}`;
    const orderId = crypto.randomUUID();

    // Seed matching order in Order Service database before validation call
    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 100,
      currency: 'USD',
      status: 'CREATED',
    });

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 100,
        currency: 'USD',
        merchantId,
        orderId,
        paymentMethod: 'card',
      });

    expect(response.status).toBe(202);
    expect(response.body).toEqual({
      paymentId: expect.any(String),
      status: 'PENDING',
    });
  });

  it('should reject requests with a 401 Unauthorized when key is unregistered', async () => {
    const idempotencyKey = `idem_auth_unreg_${Date.now()}`;
    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.invalid.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 100,
        currency: 'USD',
        merchantId: crypto.randomUUID(),
        orderId: crypto.randomUUID(),
        paymentMethod: 'card',
      });

    expect(response.status).toBe(401);
    expect(response.headers['x-request-id']).toBeDefined();
    expect(response.headers['x-correlation-id']).toBeDefined();
    expect(response.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'INVALID_API_KEY',
        message: 'Invalid API key',
        requestId: response.headers['x-request-id'],
        correlationId: response.headers['x-correlation-id'],
      }),
    });
  });

  it('should reject requests with a 401 Unauthorized when key is revoked/inactive', async () => {
    await createRevokedApiKey(MERCHANT_FIXTURES.revoked);

    const idempotencyKey = `idem_auth_revoked_${Date.now()}`;
    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.revoked.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 100,
        currency: 'USD',
        merchantId: crypto.randomUUID(),
        orderId: crypto.randomUUID(),
        paymentMethod: 'card',
      });

    expect(response.status).toBe(401);
    expect(response.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'INVALID_API_KEY',
        message: 'Invalid API key',
      }),
    });
  });

  it('should reject requests with a 403 Forbidden when merchant is disabled (INACTIVE)', async () => {
    await createTestMerchant(MERCHANT_FIXTURES.disabled);

    const idempotencyKey = `idem_auth_disabled_${Date.now()}`;
    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.disabled.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 100,
        currency: 'USD',
        merchantId: crypto.randomUUID(),
        orderId: crypto.randomUUID(),
        paymentMethod: 'card',
      });

    expect(response.status).toBe(403);
    expect(response.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'MERCHANT_DISABLED',
        message: 'Merchant status is inactive',
      }),
    });
  });

  it('should reject requests with a 401 Unauthorized when no API key header is present', async () => {
    const idempotencyKey = `idem_auth_missing_${Date.now()}`;
    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 100,
        currency: 'USD',
        merchantId: crypto.randomUUID(),
        orderId: crypto.randomUUID(),
        paymentMethod: 'card',
      });

    expect(response.status).toBe(401);
    expect(response.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'INVALID_API_KEY',
        message: 'Missing API key',
      }),
    });
  });
});
