import type { INestApplication } from '@nestjs/common';
import * as request from 'supertest';

import type { PrismaService } from '../../../apps/payment-service/src/prisma/prisma.service';
import { OutboxRepository } from '../../../apps/payment-service/src/repositories/outbox.repository';
import {
  assertOutboxPersisted,
  assertPaymentPersisted,
  assertTransactionRollback,
} from '../../helpers/database-assertions';
import { createPaymentTestApp } from '../../helpers/test-app.factory';

describe('Payment Transaction Integration', () => {
  let app: INestApplication;
  let prismaService: PrismaService;
  let outboxRepository: OutboxRepository;

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
    outboxRepository = app.get(OutboxRepository);
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

  describe('Test Case 1 — Successful Payment Transaction', () => {
    it('should atomically persist both Payment and Outbox records in a transaction', async () => {
      mockServiceClient.orderService.post.mockResolvedValue({
        valid: true,
        orderId: '3c0a5200-a000-4b0c-80a5-f00000000001',
      });

      mockServiceClient.fraudService.post.mockResolvedValue({
        approved: true,
        riskScore: 15,
      });

      const merchantId = 'c0a52000-a000-4b0c-80a5-f00000000002';
      const requestId = 'req_test_tx_1';
      const correlationId = 'corr_test_tx_1';
      const reference = 'REF-TX-001';

      const response = await request(app.getHttpServer())
        .post('/api/payments')
        .set('x-merchant-id', merchantId)
        .set('x-request-id', requestId)
        .set('x-correlation-id', correlationId)
        .send({
          amount: 5000,
          currency: 'USD',
          reference,
          paymentMethod: 'card',
        });

      expect(response.status).toBe(202);
      expect(response.body.paymentId).toBeDefined();
      expect(response.body.status).toBe('PENDING');

      const paymentId = response.body.paymentId;

      const payment = await assertPaymentPersisted(prismaService.client, {
        merchantId,
        amount: 5000,
        currency: 'USD',
        reference,
        status: 'PENDING',
        requestId,
        correlationId,
        causationId: requestId,
      });

      expect(payment.id).toBe(paymentId);

      await assertOutboxPersisted(prismaService.client, {
        aggregateId: paymentId,
        eventType: 'PaymentInitiated',
        status: 'PENDING',
        requestId,
        correlationId,
        causationId: requestId,
        payloadSchema: {
          paymentId,
          amount: 5000,
          currency: 'USD',
          merchantId,
          orderId: '3c0a5200-a000-4b0c-80a5-f00000000001',
          paymentMethod: 'card',
        },
      });
    });
  });

  describe('Test Case 3 — Forced Outbox Failure', () => {
    it('should rollback transaction and persist neither Payment nor Outbox event if Outbox insert fails', async () => {
      mockServiceClient.orderService.post.mockResolvedValue({
        valid: true,
        orderId: '3c0a5200-a000-4b0c-80a5-f00000000001',
      });

      mockServiceClient.fraudService.post.mockResolvedValue({
        approved: true,
        riskScore: 15,
      });

      jest.spyOn(outboxRepository, 'save').mockRejectedValue(new Error('Outbox write failure mock'));

      const merchantId = 'c0a52000-a000-4b0c-80a5-f00000000002';
      const requestId = 'req_test_tx_3';
      const correlationId = 'corr_test_tx_3';
      const reference = 'REF-TX-003';

      const response = await request(app.getHttpServer())
        .post('/api/payments')
        .set('x-merchant-id', merchantId)
        .set('x-request-id', requestId)
        .set('x-correlation-id', correlationId)
        .send({
          amount: 10000,
          currency: 'USD',
          reference,
          paymentMethod: 'card',
        });

      expect(response.status).toBe(500);

      await assertTransactionRollback(prismaService.client, merchantId, reference);
    });
  });
});
