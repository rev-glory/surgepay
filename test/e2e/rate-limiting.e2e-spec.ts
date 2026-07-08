import * as request from 'supertest';
import { INestApplication } from '@nestjs/common';
import * as crypto from 'crypto';

import { setupE2EEnvironment, teardownE2EEnvironment } from '../helpers/test-setup';
import { clearDatabase, createTestMerchant, createTestOrder } from '../helpers/db-helper';
import { clearRedis } from '../helpers/redis-helper';
import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';

describe('API Gateway - E2E Rate Limiting Pipeline', () => {
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

  it('should allow 100 requests and reject the 101st request with 429 Too Many Requests', async () => {
    // Seed 101 matching orders with valid UUID references in Order Service database before validation calls
    const orderIds = Array.from({ length: 101 }, () => crypto.randomUUID());
    for (let i = 0; i < 101; i++) {
      await createTestOrder({
        merchantId,
        reference: orderIds[i]!,
        amount: 100,
        currency: 'USD',
        status: 'CREATED',
      });
    }

    // 1. Send 100 requests concurrently
    const requests = [];
    for (let i = 1; i <= 100; i++) {
      const idempotencyKey = `idem_rate_e2e_${i}_${Date.now()}`;
      requests.push(
        request(app.getHttpServer())
          .post('/api/v1/payments')
          .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
          .set('idempotency-key', idempotencyKey)
          .send({
            idempotencyKey,
            amount: 100,
            currency: 'USD',
            merchantId,
            orderId: orderIds[i - 1]!,
            paymentMethod: 'card',
          })
      );
    }

    const responses = await Promise.all(requests);

    // Verify all 100 requests succeeded
    const successResponses = responses.filter((res) => res.status === 202);
    expect(successResponses.length).toBe(100);

    // Check rate limit headers for a sample response
    const firstRes = responses[0]!;
    expect(firstRes.headers['x-ratelimit-limit']).toBe('100');
    expect(firstRes.headers['x-ratelimit-remaining']).toBeDefined();
    expect(firstRes.headers['x-ratelimit-reset']).toBeDefined();

    // Verify rate limit remaining count decremented to 0 in one of the responses
    const remainings = responses.map((res) => parseInt(res.headers['x-ratelimit-remaining'] || '-1', 10));
    expect(remainings).toContain(0);

    // 2. Send the 101st request which should exceed the rate limit
    const overLimitKey = `idem_rate_e2e_101_${Date.now()}`;
    const rejectResponse = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', overLimitKey)
      .send({
        idempotencyKey: overLimitKey,
        amount: 100,
        currency: 'USD',
        merchantId,
        orderId: orderIds[100]!,
        paymentMethod: 'card',
      });

    // Verify 429 Too Many Requests
    expect(rejectResponse.status).toBe(429);
    expect(rejectResponse.headers['x-ratelimit-limit']).toBe('100');
    expect(rejectResponse.headers['x-ratelimit-remaining']).toBe('0');
    expect(rejectResponse.headers['retry-after']).toBeDefined();

    const retryAfter = parseInt(rejectResponse.headers['retry-after'] || '0', 10);
    expect(retryAfter).toBeGreaterThan(0);

    // Verify standardized error response
    expect(rejectResponse.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'RATE_LIMIT_EXCEEDED',
        message: 'Merchant rate limit exceeded.',
        requestId: rejectResponse.headers['x-request-id'],
        correlationId: rejectResponse.headers['x-correlation-id'],
      }),
    });
  });
});
