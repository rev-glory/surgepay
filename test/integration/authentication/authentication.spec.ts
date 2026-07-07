import * as request from 'supertest';
import { INestApplication } from '@nestjs/common';
import { setupIntegrationEnvironment, teardownIntegrationEnvironment } from '../helpers/test-setup';
import { clearDatabase, createTestMerchant, createRevokedApiKey } from '../helpers/db-helper';
import { clearRedis } from '../helpers/redis-helper';
import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';

describe('API Gateway - Authentication Integration', () => {
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
  });

  it('should accept a request with a valid API key and attach merchant context', async () => {
    // 1. Create a valid active merchant
    await createTestMerchant(MERCHANT_FIXTURES.active);

    // 2. Send request to gateway test endpoint
    const idempotencyKey = `idem_auth_test_${Date.now()}`;
    const response = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({ amount: 100 });

    // 3. Verify success (endpoint returns 202)
    expect(response.status).toBe(202); // HttpStatus.ACCEPTED
    expect(response.headers['x-request-id']).toBeDefined();
    expect(response.headers['x-correlation-id']).toBeDefined();
    expect(response.body).toEqual(
      expect.objectContaining({
        success: true,
        message: 'Processing completed successfully',
      }),
    );
  });

  it('should reject a request with 401 Unauthorized if the API key is invalid/unregistered', async () => {
    const idempotencyKey = `idem_auth_test_${Date.now()}`;
    const response = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.invalid.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({ amount: 100 });

    // Verify 401 response and envelope
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

  it('should reject a request with 401 Unauthorized if the API key is revoked', async () => {
    // Create revoked key merchant
    await createRevokedApiKey(MERCHANT_FIXTURES.revoked);

    const idempotencyKey = `idem_auth_test_${Date.now()}`;
    const response = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.revoked.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({ amount: 100 });

    expect(response.status).toBe(401);
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

  it('should reject a request with 403 Forbidden if the merchant is disabled', async () => {
    // Create disabled merchant
    await createTestMerchant(MERCHANT_FIXTURES.disabled);

    const idempotencyKey = `idem_auth_test_${Date.now()}`;
    const response = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('x-api-key', MERCHANT_FIXTURES.disabled.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({ amount: 100 });

    expect(response.status).toBe(403);
    expect(response.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'MERCHANT_DISABLED',
        message: 'Merchant status is inactive',
        requestId: response.headers['x-request-id'],
        correlationId: response.headers['x-correlation-id'],
      }),
    });
  });

  it('should reject a request with 401 Unauthorized if no API key header is provided', async () => {
    const idempotencyKey = `idem_auth_test_${Date.now()}`;
    const response = await request(app.getHttpServer())
      .post('/api/v1/test-idempotency')
      .set('idempotency-key', idempotencyKey)
      .send({ amount: 100 });

    expect(response.status).toBe(401);
    expect(response.body).toEqual({
      success: false,
      error: expect.objectContaining({
        code: 'INVALID_API_KEY',
        message: 'Missing API key',
        requestId: response.headers['x-request-id'],
        correlationId: response.headers['x-correlation-id'],
      }),
    });
  });
});
