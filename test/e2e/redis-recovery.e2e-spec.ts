import * as request from 'supertest';
import { INestApplication } from '@nestjs/common';
import * as crypto from 'crypto';

import { setupE2EEnvironment, teardownE2EEnvironment, getRedisContainerInstance } from '../helpers/test-setup';
import { clearDatabase, createTestMerchant } from '../helpers/db-helper';
import { clearRedis } from '../helpers/redis-helper';
import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';

describe('API Gateway - E2E Redis Restart Recovery', () => {
  let gatewayApp: INestApplication;
  let merchantId: string;

  beforeAll(async () => {
    const environment = await setupE2EEnvironment();
    gatewayApp = environment.gatewayApp;
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

  it('should recover gracefully and reconnect after Redis restarts', async () => {
    const idempotencyUrl = process.env.IDEMPOTENCY_SERVICE_URL!;
    const gatewayUrl = gatewayApp.getHttpServer();

    // 1. Initially the Idempotency Service health/readiness endpoint should be READY
    const healthCheck1 = await request(idempotencyUrl).get('/health/ready');
    expect(healthCheck1.status).toBe(200);
    expect(healthCheck1.body.status).toBe('UP');

    // 2. Perform a successful request
    const idempotencyKey1 = `idem_rec_1_${Date.now()}`;
    const response1 = await request(gatewayUrl)
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey1)
      .send({
        idempotencyKey: idempotencyKey1,
        amount: 100,
        currency: 'USD',
        merchantId,
        orderId: crypto.randomUUID(),
        paymentMethod: 'card',
      });
    expect(response1.status).toBe(202);

    // 3. Restart the Redis container
    console.log('🔄 Restarting Redis Testcontainer...');
    const redisContainer = getRedisContainerInstance();
    expect(redisContainer).toBeDefined();
    expect(redisContainer).not.toBeNull();
    await redisContainer!.restart();
    console.log('✓ Redis Testcontainer restarted.');

    // 4. Verify that we can query health check again and wait for it to report READY (with retries and backoff)
    let isReady = false;
    const maxAttempts = 15;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const healthCheck = await request(idempotencyUrl).get('/health/ready');
        if (healthCheck.status === 200 && healthCheck.body.status === 'UP') {
          isReady = true;
          console.log(`✓ Redis recovered and Idempotency Service reported UP on attempt ${attempt}.`);
          break;
        }
      } catch (err) {
        // Connection error is expected while Redis starts up
      }
      await new Promise((resolve) => setTimeout(resolve, 1000)); // wait 1s between checks
    }

    expect(isReady).toBe(true);

    // 5. Verify the Gateway processes new requests normally post-recovery
    const idempotencyKey2 = `idem_rec_2_${Date.now()}`;
    const response2 = await request(gatewayUrl)
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey2)
      .send({
        idempotencyKey: idempotencyKey2,
        amount: 200,
        currency: 'USD',
        merchantId,
        orderId: crypto.randomUUID(),
        paymentMethod: 'card',
      });
    expect(response2.status).toBe(202);
  });
});
