import type { INestApplication } from '@nestjs/common';
import * as request from 'supertest';

import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';
import { clearDatabase, createTestMerchant } from '../helpers/db-helper';
import { clearRedis } from '../helpers/redis-helper';
import { setupE2EEnvironment, teardownE2EEnvironment } from '../helpers/test-setup';

describe('API Gateway - E2E Concurrent Requests Pipeline', () => {
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
    await createTestMerchant(MERCHANT_FIXTURES.active);
  });

  it('should allow only one concurrent request to acquire lock and reject duplicate concurrent request with 409 Conflict', async () => {
    const idempotencyKey = `idem_e2e_concurrent_${Date.now()}`;
    const payload = { delay: 300, test: true };

    // 1. Send Request A which will delay 300ms downstream
    const reqA = request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    // 2. Wait a brief moment to make sure Request A arrives and acquires the lock
    await new Promise((resolve) => setTimeout(resolve, 50));

    // 3. Send Request B with same idempotency key while Request A is still in-progress
    const reqB = request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    // 4. Await both completions
    const [resA, resB] = await Promise.all([reqA, reqB]);

    // 5. Verify Request A succeeded (status 202)
    expect(resA.status).toBe(202);
    expect(resA.body.success).toBe(true);

    // 6. Verify Request B was rejected with 409 Conflict and standard error envelope
    expect(resB.status).toBe(409);
    expect(resB.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'IDEMPOTENCY_CONFLICT',
        message: 'An identical request with this Idempotency-Key is already in progress',
        requestId: resB.headers['x-request-id'],
        correlationId: resB.headers['x-correlation-id'],
      }),
    });
  });
});
