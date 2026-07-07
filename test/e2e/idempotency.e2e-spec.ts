import * as request from 'supertest';
import { INestApplication } from '@nestjs/common';
import * as crypto from 'crypto';

import { setupE2EEnvironment, teardownE2EEnvironment } from '../helpers/test-setup';
import { clearDatabase, createTestMerchant } from '../helpers/db-helper';
import { clearRedis, getIdempotencyRecord } from '../helpers/redis-helper';
import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';

describe('API Gateway - E2E Idempotency Pipeline', () => {
  let app: INestApplication;
  let merchantId: string;

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
    const merchant = await createTestMerchant(MERCHANT_FIXTURES.active);
    merchantId = merchant.merchantId;
  });

  it('should replay the cached response on duplicate requests', async () => {
    const idempotencyKey = `idem_e2e_dup_${Date.now()}`;
    const payload = {
      idempotencyKey,
      amount: 500,
      currency: 'USD',
      merchantId,
      orderId: crypto.randomUUID(),
      paymentMethod: 'card',
    };

    // 1. First request
    const response1 = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response1.status).toBe(202);

    // 2. Second request (HIT)
    const response2 = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    // Verify status, body, and header
    expect(response2.status).toBe(202);
    expect(response2.headers['idempotency-replayed']).toBe('true');
    expect(response2.body).toEqual(response1.body);
  });

  it('should reject request payload changes on the same key with 422 Unprocessable Entity', async () => {
    const idempotencyKey = `idem_e2e_mismatch_${Date.now()}`;
    const orderId = crypto.randomUUID();

    const payload1 = {
      idempotencyKey,
      amount: 100,
      currency: 'USD',
      merchantId,
      orderId,
      paymentMethod: 'card',
    };

    const payload2 = {
      idempotencyKey,
      amount: 200, // changed amount
      currency: 'USD',
      merchantId,
      orderId,
      paymentMethod: 'card',
    };

    // First request
    const response1 = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload1);

    expect(response1.status).toBe(202);

    // Second request with same key but different body
    const response2 = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload2);

    expect(response2.status).toBe(422);
    expect(response2.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'IDEMPOTENCY_CONFLICT',
        message: 'IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_REQUEST',
        requestId: response2.headers['x-request-id'],
        correlationId: response2.headers['x-correlation-id'],
      }),
    });
  });
});
