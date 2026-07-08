const blacklistedMerchantId = '99999999-9999-4999-a999-999999999999';
process.env.FRAUD_BLACKLISTED_MERCHANTS = blacklistedMerchantId;

import * as crypto from 'crypto';

import type { INestApplication } from '@nestjs/common';
import * as request from 'supertest';

import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';
import { clearDatabase, createTestMerchant, createTestOrder, getOutboxCount, getOutboxEvents, getPaymentCount, getPaymentRecords } from '../helpers/db-helper';
import { clearRedis } from '../helpers/redis-helper';
import { setupE2EEnvironment, teardownE2EEnvironment } from '../helpers/test-setup';

describe('Fraud Pre-check & Risk Screening - E2E Integration Pipeline', () => {
  let app: INestApplication;
  let environment: any;
  let merchantId: string;

  beforeAll(async () => {
    environment = await setupE2EEnvironment();
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

  it('Scenario 1: Low-risk payment -> approved, creates payment and returns 202 Accepted', async () => {
    const idempotencyKey = `idem_fraud_low_${Date.now()}`;
    const orderId = crypto.randomUUID();

    const testOrder = await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 5000,
      currency: 'USD',
      status: 'CREATED',
    });

    const payload = {
      idempotencyKey,
      amount: 5000,
      currency: 'USD',
      merchantId,
      orderId,
      paymentMethod: 'card',
    };

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response.status).toBe(202);
    expect(response.body).toEqual({
      paymentId: expect.any(String),
      status: 'PENDING',
    });

    const paymentId = response.body.paymentId;

    // Verify Payment table has 1 row
    const count = await getPaymentCount(merchantId, orderId);
    expect(count).toBe(1);

    // Verify OutboxEvent table has 1 row
    const outboxCount = await getOutboxCount(paymentId);
    expect(outboxCount).toBe(1);

    // Verify Payment table properties
    const paymentRecords = await getPaymentRecords(merchantId, orderId);
    const paymentRecord = paymentRecords[0];
    expect(paymentRecord).toBeDefined();
    expect(paymentRecord.requestId).toBeDefined();
    expect(paymentRecord.correlationId).toBeDefined();
    expect(paymentRecord.causationId).toBeDefined();
    expect(paymentRecord.createdBy).toBe(merchantId);
    expect(paymentRecord.source).toBe('GATEWAY');


    // Verify OutboxEvent details
    const outboxRecords = await getOutboxEvents(paymentId);
    const outboxRecord = outboxRecords[0];
    expect(outboxRecord).toBeDefined();
    expect(outboxRecord.status).toBe('PENDING');
    expect(outboxRecord.retryCount).toBe(0);
    expect(outboxRecord.publishedAt).toBeNull();
    expect(outboxRecord.requestId).toBe(paymentRecord.requestId);
    expect(outboxRecord.correlationId).toBe(paymentRecord.correlationId);
    expect(outboxRecord.causationId).toBe(paymentRecord.causationId);

    // Verify complete event envelope
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
      amount: 5000,
      currency: 'USD',
      merchantId,
      orderId: testOrder.id,
      paymentMethod: 'card',
    });
  });

  it('Scenario 2.1: High-risk payment -> amount threshold exceeded, rejected and returns 403 Forbidden with PAYMENT_BLOCKED', async () => {
    const idempotencyKey = `idem_fraud_amount_${Date.now()}`;
    const orderId = crypto.randomUUID();
    const excessiveAmount = 12000000;

    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: excessiveAmount,
      currency: 'USD',
      status: 'CREATED',
    });

    const payload = {
      idempotencyKey,
      amount: excessiveAmount,
      currency: 'USD',
      merchantId,
      orderId,
      paymentMethod: 'card',
    };

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response.status).toBe(403);
    expect(response.body.success).toBe(false);
    expect(response.body.error.code).toBe('PAYMENT_BLOCKED');
    expect(response.body.error.message).toContain('Payment rejected by fraud rules');

    const count = await getPaymentCount(merchantId, orderId);
    expect(count).toBe(0);
  });

  it('Scenario 2.2: High-risk payment -> unsupported currency, rejected and returns 403 Forbidden with PAYMENT_BLOCKED', async () => {
    const idempotencyKey = `idem_fraud_currency_${Date.now()}`;
    const orderId = crypto.randomUUID();

    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 5000,
      currency: 'JPY',
      status: 'CREATED',
    });

    const payload = {
      idempotencyKey,
      amount: 5000,
      currency: 'JPY',
      merchantId,
      orderId,
      paymentMethod: 'card',
    };

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response.status).toBe(403);
    expect(response.body.success).toBe(false);
    expect(response.body.error.code).toBe('PAYMENT_BLOCKED');

    const count = await getPaymentCount(merchantId, orderId);
    expect(count).toBe(0);
  });

  it('Scenario 2.3: High-risk payment -> blacklisted merchant, rejected and returns 403 Forbidden with PAYMENT_BLOCKED', async () => {
    const idempotencyKey = `idem_fraud_blacklist_${Date.now()}`;
    const orderId = crypto.randomUUID();

    // Create the blacklisted merchant in DB with a custom API key to authenticate
    const customMerchantApiKey = 'blacklisted-merchant-api-key-test';
    const blacklistedMerchant = await createTestMerchant({
      apiKey: customMerchantApiKey,
      name: 'Blacklisted Merchant',
      email: 'blacklisted@surgepay.com',
      permissions: ['payment:create'],
      webhookEnabled: true,
      status: 'ACTIVE',
      merchantId: blacklistedMerchantId,
    });

    await createTestOrder({
      merchantId: blacklistedMerchant.merchantId,
      reference: orderId,
      amount: 5000,
      currency: 'USD',
      status: 'CREATED',
    });

    const payload = {
      idempotencyKey,
      amount: 5000,
      currency: 'USD',
      merchantId: blacklistedMerchant.merchantId,
      orderId,
      paymentMethod: 'card',
    };

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', customMerchantApiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response.status).toBe(403);
    expect(response.body.success).toBe(false);
    expect(response.body.error.code).toBe('PAYMENT_BLOCKED');

    const count = await getPaymentCount(blacklistedMerchant.merchantId, orderId);
    expect(count).toBe(0);
  });

  it('Scenario 4: Fraud Pre-check service timeout (> 2 seconds) -> timeout handled and returns 503 Service Unavailable', async () => {
    const idempotencyKey = `idem_fraud_timeout_${Date.now()}`;
    const orderId = crypto.randomUUID();

    // Seed a specific timeout merchant ID: '00000000-0000-4000-a000-000000000000'
    const timeoutMerchantId = '00000000-0000-4000-a000-000000000000';
    const customMerchantApiKey = 'timeout-merchant-api-key-test';
    await createTestMerchant({
      apiKey: customMerchantApiKey,
      name: 'Timeout Merchant',
      email: 'timeout@surgepay.com',
      permissions: ['payment:create'],
      webhookEnabled: true,
      status: 'ACTIVE',
      merchantId: timeoutMerchantId,
    });

    await createTestOrder({
      merchantId: timeoutMerchantId,
      reference: orderId,
      amount: 5000,
      currency: 'USD',
      status: 'CREATED',
    });

    const payload = {
      idempotencyKey,
      amount: 5000,
      currency: 'USD',
      merchantId: timeoutMerchantId,
      orderId,
      paymentMethod: 'card',
    };

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', customMerchantApiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response.status).toBe(503);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('Fraud pre-check service timed out');

    const count = await getPaymentCount(timeoutMerchantId, orderId);
    expect(count).toBe(0);
  });

  it('Scenario 3: Fraud Pre-check service unavailable -> returns 503 Service Unavailable', async () => {
    const idempotencyKey = `idem_fraud_unavailable_${Date.now()}`;
    const orderId = crypto.randomUUID();

    await createTestOrder({
      merchantId,
      reference: orderId,
      amount: 5000,
      currency: 'USD',
      status: 'CREATED',
    });

    const payload = {
      idempotencyKey,
      amount: 5000,
      currency: 'USD',
      merchantId,
      orderId,
      paymentMethod: 'card',
    };

    // Close the fraud app to simulate unavailable downstream server
    await environment.fraudApp.close();

    const response = await request(app.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response.status).toBe(503);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('Fraud pre-check service is unavailable');

    const count = await getPaymentCount(merchantId, orderId);
    expect(count).toBe(0);
  });
});
