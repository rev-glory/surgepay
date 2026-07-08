import type { INestApplication } from '@nestjs/common';
import * as request from 'supertest';

import { DownstreamResponseException } from '@surgepay/common-http';

import type { PrismaService } from '../../../apps/payment-service/src/prisma/prisma.service';
import { assertTransactionRollback } from '../../helpers/database-assertions';
import { createPaymentTestApp } from '../../helpers/test-app.factory';

describe('Payment Validation Integration', () => {
  let app: INestApplication;
  let prismaService: PrismaService;

  const mockServiceClient = {
    orderService: {
      post: jest.fn(),
    },
    fraudService: {
      post: jest.fn(),
    },
  };

  beforeAll(async () => {
    const testEnv = await createPaymentTestApp(mockServiceClient);
    app = testEnv.app;
    prismaService = testEnv.prismaService;
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    jest.clearAllMocks();
    jest.restoreAllMocks();
    await prismaService.client.$executeRawUnsafe('TRUNCATE TABLE "payment"."Payment" CASCADE;');
    await prismaService.client.$executeRawUnsafe('TRUNCATE TABLE "payment"."OutboxEvent" CASCADE;');
  });

  describe('Test Case 2 — Duplicate Merchant Reference', () => {
    it('should fail-fast with DUPLICATE_PAYMENT_REFERENCE error and perform zero database updates on duplicate reference', async () => {
      mockServiceClient.orderService.post.mockResolvedValue({
        valid: true,
        orderId: '3c0a5200-a000-4b0c-80a5-f00000000001',
      });
      mockServiceClient.fraudService.post.mockResolvedValue({
        approved: true,
        riskScore: 15,
      });

      const merchantId = 'c0a52000-a000-4b0c-80a5-f00000000002';
      const reference = 'REF-DUP-001';

      const res1 = await request(app.getHttpServer())
        .post('/api/payments')
        .set('x-merchant-id', merchantId)
        .send({
          amount: 5000,
          currency: 'USD',
          reference,
        });
      expect(res1.status).toBe(202);

      mockServiceClient.orderService.post.mockClear();
      mockServiceClient.fraudService.post.mockClear();

      const res2 = await request(app.getHttpServer())
        .post('/api/payments')
        .set('x-merchant-id', merchantId)
        .send({
          amount: 5000,
          currency: 'USD',
          reference,
        });

      expect(res2.status).toBe(409);
      expect(res2.body.success).toBe(false);
      expect(res2.body.error.code).toBe('DUPLICATE_PAYMENT_REFERENCE');

      expect(mockServiceClient.orderService.post).not.toHaveBeenCalled();
      expect(mockServiceClient.fraudService.post).not.toHaveBeenCalled();

      const paymentsCount = await prismaService.client.payment.count();
      const outboxCount = await prismaService.client.outboxEvent.count();
      expect(paymentsCount).toBe(1);
      expect(outboxCount).toBe(1);
    });
  });

  describe('Test Case 4 — Fraud Rejection', () => {
    it('should reject with FRAUD_REJECTED error and perform zero database writes if fraud precheck rejects', async () => {
      mockServiceClient.orderService.post.mockResolvedValue({
        valid: true,
        orderId: '3c0a5200-a000-4b0c-80a5-f00000000001',
      });
      mockServiceClient.fraudService.post.mockResolvedValue({
        approved: false,
        riskScore: 95,
        reason: 'Velocity check failed',
      });

      const merchantId = 'c0a52000-a000-4b0c-80a5-f00000000002';
      const reference = 'REF-FRAUD-001';

      const response = await request(app.getHttpServer())
        .post('/api/payments')
        .set('x-merchant-id', merchantId)
        .send({
          amount: 10000,
          currency: 'USD',
          reference,
        });

      expect(response.status).toBe(403);
      expect(response.body.success).toBe(false);
      expect(response.body.error.code).toBe('FRAUD_REJECTED');

      await assertTransactionRollback(prismaService.client, merchantId, reference);
    });
  });

  describe('Test Case 5 — Invalid Order', () => {
    it('should fail with ORDER_NOT_FOUND error and perform zero database writes if Order Service returns 404', async () => {
      const downstreamException = new DownstreamResponseException(
        404,
        { error: 'Order not found' },
        {},
        { service: 'order-service', url: '/api/v1/internal/orders/validate', method: 'POST' },
      );
      mockServiceClient.orderService.post.mockRejectedValue(downstreamException);

      const merchantId = 'c0a52000-a000-4b0c-80a5-f00000000002';
      const reference = 'REF-ORDER-001';

      const response = await request(app.getHttpServer())
        .post('/api/payments')
        .set('x-merchant-id', merchantId)
        .send({
          amount: 15000,
          currency: 'USD',
          reference,
        });

      expect(response.status).toBe(404);
      expect(response.body.success).toBe(false);
      expect(response.body.error.code).toBe('ORDER_NOT_FOUND');

      await assertTransactionRollback(prismaService.client, merchantId, reference);
    });
  });
});
