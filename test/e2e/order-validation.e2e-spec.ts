import * as crypto from 'crypto';

import type { INestApplication } from '@nestjs/common';
import * as request from 'supertest';

import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';
import { clearDatabase, createTestMerchant, createTestOrder } from '../helpers/db-helper';
import { setupE2EEnvironment, teardownE2EEnvironment } from '../helpers/test-setup';

describe('Order Service - E2E Internal Order Validation Pipeline', () => {
  let orderApp: INestApplication;
  let merchantId: string;

  beforeAll(async () => {
    const environment = await setupE2EEnvironment();
    // Retrieve the dynamic booted Order Service instance
    orderApp = (environment as any).orderApp;
  });

  afterAll(async () => {
    await teardownE2EEnvironment();
  });

  beforeEach(async () => {
    await clearDatabase();
    const merchant = await createTestMerchant(MERCHANT_FIXTURES.active);
    merchantId = merchant.merchantId;
  });

  it('should successfully validate a valid CREATED order and return 200 OK', async () => {
    const reference = 'ORDER-1001';
    await createTestOrder({
      merchantId,
      reference,
      amount: 5000,
      currency: 'INR',
      status: 'CREATED',
    });

    const response = await request(orderApp.getHttpServer())
      .post('/api/v1/internal/orders/validate')
      .send({
        merchantId,
        reference,
        amount: 5000,
        currency: 'INR',
      });

    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      valid: true,
      orderId: expect.any(String),
    });
  });

  it('should return 404 Not Found when order does not exist', async () => {
    const response = await request(orderApp.getHttpServer())
      .post('/api/v1/internal/orders/validate')
      .send({
        merchantId,
        reference: 'NON-EXISTENT-ORDER',
        amount: 5000,
        currency: 'INR',
      });

    expect(response.status).toBe(404);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('not found');
  });

  it('should return 403 Forbidden when order is owned by a different merchant', async () => {
    const reference = 'ORDER-1002';
    await createTestOrder({
      merchantId,
      reference,
      amount: 5000,
      currency: 'INR',
      status: 'CREATED',
    });

    const otherMerchantId = crypto.randomUUID();
    const response = await request(orderApp.getHttpServer())
      .post('/api/v1/internal/orders/validate')
      .send({
        merchantId: otherMerchantId, // Merchant mismatch
        reference,
        amount: 5000,
        currency: 'INR',
      });

    expect(response.status).toBe(403);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('Merchant does not own this order');
  });

  it('should return 422 Unprocessable Entity when order amount does not match', async () => {
    const reference = 'ORDER-1003';
    await createTestOrder({
      merchantId,
      reference,
      amount: 5000,
      currency: 'INR',
      status: 'CREATED',
    });

    const response = await request(orderApp.getHttpServer())
      .post('/api/v1/internal/orders/validate')
      .send({
        merchantId,
        reference,
        amount: 1000, // Amount mismatch (1000 vs 5000)
        currency: 'INR',
      });

    expect(response.status).toBe(422);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('Amount mismatch');
  });

  it('should return 422 Unprocessable Entity when order currency does not match', async () => {
    const reference = 'ORDER-1004';
    await createTestOrder({
      merchantId,
      reference,
      amount: 5000,
      currency: 'INR',
      status: 'CREATED',
    });

    const response = await request(orderApp.getHttpServer())
      .post('/api/v1/internal/orders/validate')
      .send({
        merchantId,
        reference,
        amount: 5000,
        currency: 'USD', // Currency mismatch (USD vs INR)
      });

    expect(response.status).toBe(422);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('Currency mismatch');
  });

  it('should return 409 Conflict when order is already PAID', async () => {
    const reference = 'ORDER-1005';
    await createTestOrder({
      merchantId,
      reference,
      amount: 5000,
      currency: 'INR',
      status: 'PAID', // Already PAID status
    });

    const response = await request(orderApp.getHttpServer())
      .post('/api/v1/internal/orders/validate')
      .send({
        merchantId,
        reference,
        amount: 5000,
        currency: 'INR',
      });

    expect(response.status).toBe(409);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('already paid');
  });

  it('should return 409 Conflict when order is CANCELLED', async () => {
    const reference = 'ORDER-1006';
    await createTestOrder({
      merchantId,
      reference,
      amount: 5000,
      currency: 'INR',
      status: 'CANCELLED', // CANCELLED status
    });

    const response = await request(orderApp.getHttpServer())
      .post('/api/v1/internal/orders/validate')
      .send({
        merchantId,
        reference,
        amount: 5000,
        currency: 'INR',
      });

    expect(response.status).toBe(409);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('cancelled');
  });

  it('should return 409 Conflict when order is REFUNDED', async () => {
    const reference = 'ORDER-1007';
    await createTestOrder({
      merchantId,
      reference,
      amount: 5000,
      currency: 'INR',
      status: 'REFUNDED', // REFUNDED status
    });

    const response = await request(orderApp.getHttpServer())
      .post('/api/v1/internal/orders/validate')
      .send({
        merchantId,
        reference,
        amount: 5000,
        currency: 'INR',
      });

    expect(response.status).toBe(409);
    expect(response.body.success).toBe(false);
    expect(response.body.error.message).toContain('refunded');
  });
});
