import * as request from 'supertest';
import { INestApplication } from '@nestjs/common';
import { setupIntegrationEnvironment, teardownIntegrationEnvironment } from '../helpers/test-setup';
import { clearDatabase, createTestMerchant } from '../helpers/db-helper';
import { clearRedis } from '../helpers/redis-helper';
import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';

describe('API Gateway - Rate Limiting Integration', () => {
  let app: INestApplication;

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
    await createTestMerchant(MERCHANT_FIXTURES.active);
  });

  it('should allow 100 requests and reject the 101st request with 429 Too Many Requests', async () => {
    // 1. Send 100 requests concurrently
    const requests = [];
    for (let i = 1; i <= 100; i++) {
      const idempotencyKey = `idem_rate_test_${i}_${Date.now()}`;
      requests.push(
        request(app.getHttpServer())
          .post('/api/v1/test-idempotency')
          .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
          .set('idempotency-key', idempotencyKey)
          .send({ amount: 100 })
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

    // 2. Send the 101st request which should exceed rate limit
    const overLimitKey = `idem_rate_test_101_${Date.now()}`;
    const rejectResponse = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', overLimitKey)
      .send({ amount: 100 });

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
