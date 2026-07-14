import * as crypto from 'crypto';

import type { INestApplication } from '@nestjs/common';
import * as request from 'supertest';

import { OutboxRepository } from '../../apps/payment-service/src/repositories/outbox.repository';
import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';
import {
  clearDatabase,
  createTestMerchant,
  createTestOrder,
  getOutboxCount,
  getOutboxEvents,
  getPaymentCount,
  getPaymentRecords,
} from '../helpers/db-helper';
import { clearRedis } from '../helpers/redis-helper';
import { setupE2EEnvironment, teardownE2EEnvironment } from '../helpers/test-setup';

describe('Payment Pipeline - E2E Synchronous Pipeline', () => {
  let app: INestApplication;
  let paymentApp: INestApplication;
  let merchantId: string;

  beforeAll(async () => {
    const environment = await setupE2EEnvironment();
    app = environment.gatewayApp!;
    paymentApp = environment.paymentApp!;
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

  it('Scenario 1: Positive Test - Valid payment request pipeline executes synchronously and atomically', async () => {
    const idempotencyKey = `idem_pipeline_ok_${Date.now()}`;
    const orderId = crypto.randomUUID();

    // 1. Seed matching order in Order Service database before validation call
    const testOrder = await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 15000,
      currency: 'USD',
      status: 'CREATED',
    });

    const payload = {
      idempotencyKey,
      amount: 15000,
      currency: 'USD',
      merchantId,
      orderId,
      paymentMethod: 'card',
    };

    // 2. Execute E2E Request
    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    // 3. Assert HTTP response
    expect(response.status).toBe(202);
    expect(response.headers['x-request-id']).toBeDefined();
    expect(response.headers['x-correlation-id']).toBeDefined();
    expect(response.body).toEqual({
      paymentId: expect.any(String),
      status: 'PENDING',
    });

    const paymentId = response.body.paymentId;

    // 4. Assert database persistence (Payment + Outbox committed atomically)
    const paymentCount = await getPaymentCount(merchantId, orderId);
    expect(paymentCount).toBe(1);

    const outboxCount = await getOutboxCount(paymentId);
    expect(outboxCount).toBe(1);

    // 5. Verify Tracing Metadata Propagation
    const paymentRecords = await getPaymentRecords(merchantId, orderId);
    const paymentRecord = paymentRecords[0];
    expect(paymentRecord).toBeDefined();
    expect(paymentRecord.status).toBe('PENDING');
    expect(paymentRecord.requestId).toBe(response.headers['x-request-id']);
    expect(paymentRecord.correlationId).toBe(response.headers['x-correlation-id']);
    expect(paymentRecord.causationId).toBeDefined();
    expect(paymentRecord.merchantId).toBe(merchantId);

    const outboxRecords = await getOutboxEvents(paymentId);
    const outboxRecord = outboxRecords[0];
    expect(outboxRecord).toBeDefined();
    expect(['PENDING', 'PUBLISHING', 'PUBLISHED']).toContain(outboxRecord.status);
    expect(outboxRecord.requestId).toBe(paymentRecord.requestId);
    expect(outboxRecord.correlationId).toBe(paymentRecord.correlationId);
    expect(outboxRecord.causationId).toBe(paymentRecord.causationId);
    expect(outboxRecord.aggregateId).toBe(paymentId);
    expect(outboxRecord.eventType).toBe('PaymentInitiated');

    // 6. Verify Event Envelope payload details
    const envelope = outboxRecord.payload;
    expect(envelope).toBeDefined();
    expect(envelope.eventId).toBe(outboxRecord.id);
    expect(envelope.eventType).toBe('PaymentInitiated');
    expect(envelope.version).toBe(1);
    expect(envelope.correlationId).toBe(paymentRecord.correlationId);
    expect(envelope.causationId).toBe(paymentRecord.causationId);
    expect(envelope.requestId).toBe(paymentRecord.requestId);
    expect(envelope.timestamp).toBeDefined();
    expect(envelope.payload).toEqual({
      paymentId: paymentId,
      amount: 15000,
      currency: 'USD',
      merchantId,
      orderId: testOrder.id,
      paymentMethod: 'card',
    });
  });

  it('Scenario 2: Idempotency - Duplicate request returns cached response and executes only once', async () => {
    const idempotencyKey = `idem_pipeline_double_${Date.now()}`;
    const orderId = crypto.randomUUID();

    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 20000,
      currency: 'USD',
      status: 'CREATED',
    });

    const payload = {
      idempotencyKey,
      amount: 20000,
      currency: 'USD',
      merchantId,
      orderId,
      paymentMethod: 'card',
    };

    // 1st request (MISS)
    const response1 = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response1.status).toBe(202);

    // 2nd request (COMPLETED Cache HIT)
    const response2 = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response2.status).toBe(202);
    expect(response2.body).toEqual(response1.body);
    expect(response2.headers['idempotency-replayed']).toBe('true');

    // Assert that only 1 payment and 1 outbox record were created
    const paymentCount = await getPaymentCount(merchantId, orderId);
    expect(paymentCount).toBe(1);

    const paymentId = response1.body.paymentId;
    const outboxCount = await getOutboxCount(paymentId);
    expect(outboxCount).toBe(1);
  });

  it('Scenario 3: Negative - Invalid merchant returns 401 Unauthorized', async () => {
    const idempotencyKey = `idem_pipeline_auth_${Date.now()}`;
    const orderId = crypto.randomUUID();

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.invalid.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 5000,
        currency: 'USD',
        merchantId: crypto.randomUUID(),
        orderId,
        paymentMethod: 'card',
      });

    expect(response.status).toBe(401);
    expect(response.body.success).toBe(false);
    expect(response.body.error.code).toBe('INVALID_API_KEY');

    // Verify nothing persisted
    const count = await getPaymentCount(merchantId, orderId);
    expect(count).toBe(0);
  });

  it('Scenario 4: Negative - Missing/invalid order validation returns 404 Not Found', async () => {
    const idempotencyKey = `idem_pipeline_missing_${Date.now()}`;
    const orderId = crypto.randomUUID();

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 5000,
        currency: 'USD',
        merchantId,
        orderId,
        paymentMethod: 'card',
      });

    expect(response.status).toBe(404);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('not found');

    // Verify nothing persisted
    const count = await getPaymentCount(merchantId, orderId);
    expect(count).toBe(0);
  });

  it('Scenario 5: Negative - Fraud rejected returns 403 Forbidden with PAYMENT_BLOCKED', async () => {
    const idempotencyKey = `idem_pipeline_fraud_${Date.now()}`;
    const orderId = crypto.randomUUID();
    const excessiveAmount = 12000000; // Trigger high amount fraud rule

    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: excessiveAmount,
      currency: 'USD',
      status: 'CREATED',
    });

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: excessiveAmount,
        currency: 'USD',
        merchantId,
        orderId,
        paymentMethod: 'card',
      });

    expect(response.status).toBe(403);
    expect(response.body.success).toBe(false);
    expect(response.body.error.code).toBe('PAYMENT_BLOCKED');

    // Verify nothing persisted
    const count = await getPaymentCount(merchantId, orderId);
    expect(count).toBe(0);
  });

  it('Scenario 6: Negative - Duplicate payment reference returns 409 Conflict', async () => {
    const idempotencyKey1 = `idem_pipeline_dup_1_${Date.now()}`;
    const idempotencyKey2 = `idem_pipeline_dup_2_${Date.now()}`;
    const orderId = crypto.randomUUID();

    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 1000,
      currency: 'USD',
      status: 'CREATED',
    });

    // Create first payment successfully
    const response1 = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey1)
      .send({
        idempotencyKey: idempotencyKey1,
        amount: 1000,
        currency: 'USD',
        merchantId,
        orderId,
        paymentMethod: 'card',
      });

    expect(response1.status).toBe(202);

    // Try creating another payment with same reference/orderId (but different idempotency key to bypass gateway check)
    const response2 = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey2)
      .send({
        idempotencyKey: idempotencyKey2,
        amount: 1000,
        currency: 'USD',
        merchantId,
        orderId,
        paymentMethod: 'card',
      });

    expect(response2.status).toBe(409);
    expect(response2.body.success).toBe(false);

    // Assert that only 1 payment was created
    const count = await getPaymentCount(merchantId, orderId);
    expect(count).toBe(1);
  });

  it('Scenario 7: Transaction Rollback - Atomic rollback on Outbox persistence failure', async () => {
    const idempotencyKey = `idem_pipeline_rollback_${Date.now()}`;
    const orderId = crypto.randomUUID();

    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 1000,
      currency: 'USD',
      status: 'CREATED',
    });

    // Retrieve OutboxRepository from the booted Payment Service context
    const outboxRepository = paymentApp.get(OutboxRepository);

    // Simulate an Outbox persistence failure at the repository boundary
    const saveSpy = jest.spyOn(outboxRepository, 'save').mockRejectedValueOnce(new Error('Simulated database write error'));

    try {
      const response = await request(app.getHttpServer())
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

      // The request should fail due to internal rollback
      expect(response.status).toBe(500);
    } finally {
      saveSpy.mockRestore();
    }

    // Verify that NO payment and NO outbox record were committed
    const paymentCount = await getPaymentCount(merchantId, orderId);
    expect(paymentCount).toBe(0);
  });
});
