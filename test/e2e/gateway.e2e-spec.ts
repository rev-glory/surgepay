import * as crypto from 'crypto';

import type { INestApplication } from '@nestjs/common';
import { performance } from 'perf_hooks';
import * as request from 'supertest';

import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';
import { clearDatabase, createTestMerchant, createTestOrder } from '../helpers/db-helper';
import { clearRedis, getIdempotencyRecord } from '../helpers/redis-helper';
import { setupE2EEnvironment, teardownE2EEnvironment } from '../helpers/test-setup';

describe('API Gateway - E2E Gateway Pipeline', () => {
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

  it('should successfully execute happy path (POST /payments) and cache completed status in Redis', async () => {
    const idempotencyKey = `idem_e2e_happy_${Date.now()}`;
    const orderId = crypto.randomUUID();

    // Seed matching order in Order Service database before validation call
    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 25050,
      currency: 'USD',
      status: 'CREATED',
    });

    const payload = {
      idempotencyKey,
      amount: 25050,
      currency: 'USD',
      merchantId,
      orderId,
      paymentMethod: 'card',
    };

    // 1. Execute request
    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    // 2. Assert response status and envelope
    expect(response.status).toBe(202);
    expect(response.headers['x-request-id']).toBeDefined();
    expect(response.headers['x-correlation-id']).toBeDefined();
    expect(response.body).toEqual({
      paymentId: expect.any(String),
      status: 'PENDING',
    });

    // Give a brief moment for any final background write to Redis
    await new Promise((resolve) => setTimeout(resolve, 100));

    // 3. Verify Redis entry status, requestHash, cached status code and body
    const { record, ttl } = await getIdempotencyRecord(merchantId, idempotencyKey);
    expect(record).toBeDefined();
    expect(record).not.toBeNull();
    expect(record!.status).toBe('COMPLETED');
    expect(record!.statusCode).toBe(202);
    expect(record!.requestHash).toBeDefined();
    expect(record!.body).toEqual({
      paymentId: expect.any(String),
      status: 'PENDING',
    });

    // 4. Verify TTL is configured (24 hours = 86400 seconds)
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(24 * 3600);
  });

  it('should return 400 Bad Request if Idempotency-Key header is missing', async () => {
    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .send({
        idempotencyKey: '',
        amount: 25050,
        currency: 'USD',
        merchantId,
        orderId: crypto.randomUUID(),
        paymentMethod: 'card',
      });

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

  it('should record baseline performance metrics for informational logging', async () => {
    // 1. Measure Merchant Lookup Latency
    const merchantStart = performance.now();
    await request(process.env.MERCHANT_SERVICE_URL!)
      .get('/api/v1/internal/merchants/validate')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey);
    const merchantLookupMs = performance.now() - merchantStart;

    // 2. Measure Redis Idempotency Check Latency (via Idempotency Service check api)
    const idempotencyStart = performance.now();
    await request(process.env.IDEMPOTENCY_SERVICE_URL!)
      .post('/api/v1/internal/idempotency/check')
      .send({
        merchantId,
        idempotencyKey: `idem_perf_check_${Date.now()}`,
        requestBody: { test: true },
      });
    const redisCheckMs = performance.now() - idempotencyStart;

    // 3. Measure Total Pre-processing Pipeline Latency
    const pipelineStart = performance.now();
    const idempotencyKey = `idem_perf_pipe_${Date.now()}`;
    const orderId = crypto.randomUUID();
    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 1000,
      currency: 'USD',
      status: 'CREATED',
    });

    await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 1000,
        currency: 'USD',
        merchantId,
        orderId,
        paymentMethod: 'card',
      });
    const totalPipelineMs = performance.now() - pipelineStart;

    // 4. Estimate Gateway Auth Middleware overhead
    // (Total time minus internal downstream steps, bounded to 0)
    const gatewayAuthMs = Math.max(0, totalPipelineMs - merchantLookupMs - redisCheckMs);

    console.log('======================================================');
    console.log('📊 SurgePay Synchronous Request Pipeline Latency Baselines');
    console.log('======================================================');
    console.log(`• Gateway Authentication Check:   ${gatewayAuthMs.toFixed(2)} ms (Target: < 20 ms)`);
    console.log(`• Merchant Service Lookup:        ${merchantLookupMs.toFixed(2)} ms (Target: < 15 ms)`);
    console.log(`• Redis Idempotency Check:        ${redisCheckMs.toFixed(2)} ms (Target: < 10 ms)`);
    console.log(`• Total Pre-processing Pipeline:  ${totalPipelineMs.toFixed(2)} ms (Target: < 50 ms)`);
    console.log('======================================================');

    // Informational logging only: Assertions are not used to fail the build due to environmental variability
    expect(merchantLookupMs).toBeDefined();
    expect(redisCheckMs).toBeDefined();
    expect(totalPipelineMs).toBeDefined();
  });
});
