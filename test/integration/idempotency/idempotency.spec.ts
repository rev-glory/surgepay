import type { INestApplication } from '@nestjs/common';
import * as request from 'supertest';

import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';
import { clearDatabase, createTestMerchant } from '../helpers/db-helper';
import { clearRedis, getIdempotencyRecord } from '../helpers/redis-helper';
import { setupIntegrationEnvironment, teardownIntegrationEnvironment } from '../helpers/test-setup';

describe('API Gateway - Idempotency Integration', () => {
  let app: INestApplication;
  let merchantId: string;

  beforeAll(async () => {
    const environment = await setupIntegrationEnvironment();
    app = environment.gatewayApp;
  });

  afterAll(async () => {
    await teardownIntegrationEnvironment();
  });

  beforeEach(async () => {
    await clearDatabase();
    await clearRedis();
    const merchant = await createTestMerchant(MERCHANT_FIXTURES.active);
    merchantId = merchant.merchantId;
  });

  it('should process a request on a MISS and cache the response in Redis with TTL', async () => {
    const idempotencyKey = `idem_test_${Date.now()}`;
    const payload = { amount: 200, currency: 'USD' };

    // 1. Initial Request (MISS)
    const response = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response.status).toBe(202); // HttpStatus.ACCEPTED
    expect(response.headers['idempotency-replayed']).toBeUndefined();
    expect(response.body.success).toBe(true);

    // Give background completion a brief moment to write to Redis
    await new Promise((resolve) => setTimeout(resolve, 100));

    // 2. Direct Redis Verification
    const { record, ttl } = await getIdempotencyRecord(merchantId, idempotencyKey);
    expect(record).toBeDefined();
    expect(record).not.toBeNull();
    expect(record!.status).toBe('COMPLETED');
    expect(record!.statusCode).toBe(202);
    expect(record!.requestHash).toBeDefined();
    
    // Redis record body check
    expect(record!.body).toEqual(expect.objectContaining({
      success: true,
      message: 'Processing completed successfully',
    }));

    // TTL check (must be set to 24 hours = 86400 seconds)
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(24 * 3600);
  });

  it('should replay a cached response on a duplicate request', async () => {
    const idempotencyKey = `idem_test_${Date.now()}`;
    const payload = { amount: 350, currency: 'EUR' };

    // First Request
    const response1 = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response1.status).toBe(202);

    // Second Request (HIT)
    const response2 = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    // Verify status, body, and header
    expect(response2.status).toBe(202);
    expect(response2.headers['idempotency-replayed']).toBe('true');
    expect(response2.body).toEqual(response1.body);
  });

  it('should return 400 Bad Request when Idempotency-Key header is missing for mutating requests', async () => {
    const response = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .send({ amount: 100 });

    expect(response.status).toBe(400);
    expect(response.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'MISSING_IDEMPOTENCY_KEY',
        message: 'Missing or empty Idempotency-Key header',
        requestId: response.headers['x-request-id'],
        correlationId: response.headers['x-correlation-id'],
      }),
    });
  });

  it('should reject duplicate request key usage with a different body payload (422 Unprocessable Entity)', async () => {
    const idempotencyKey = `idem_test_${Date.now()}`;
    
    // First request
    const response1 = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({ amount: 100, currency: 'USD' });

    expect(response1.status).toBe(202);

    // Second request with same key but different body
    const response2 = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({ amount: 200, currency: 'USD' }); // changed amount from 100 to 200

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
