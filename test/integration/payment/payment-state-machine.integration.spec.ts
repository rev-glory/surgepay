import type { INestApplication } from '@nestjs/common';

import { InvalidPaymentStateTransitionException } from '@surgepay/common';

import { PaymentEntity } from '../../../apps/payment-service/src/entities/payment.entity';
import { PaymentStatus } from '../../../apps/payment-service/src/generated/client';
import type { PrismaService } from '../../../apps/payment-service/src/prisma/prisma.service';
import { PaymentRepository } from '../../../apps/payment-service/src/repositories/payment.repository';
import { createPaymentTestApp } from '../../helpers/test-app.factory';

describe('Payment State Machine Integration', () => {
  let app: INestApplication;
  let prismaService: PrismaService;
  let paymentRepository: PaymentRepository;

  const mockServiceClient = {
    orderService: { post: jest.fn() },
    fraudService: { post: jest.fn() },
  };

  beforeAll(async () => {
    const testEnv = await createPaymentTestApp(mockServiceClient);
    app = testEnv.app;
    prismaService = testEnv.prismaService;
    paymentRepository = app.get(PaymentRepository);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    await prismaService.client.$executeRawUnsafe('TRUNCATE TABLE "payment"."Payment" CASCADE;');
    await prismaService.client.$executeRawUnsafe('TRUNCATE TABLE "payment"."OutboxEvent" CASCADE;');
  });

  it('should successfully transition through PENDING -> PROCESSING -> COMPLETED', async () => {
    const payment = PaymentEntity.create({
      merchantId: 'c0a52000-a000-4b0c-80a5-f00000000002',
      amount: 2500,
      currency: 'USD',
      reference: 'REF-STATE-1',
      requestId: 'req-state-1',
      correlationId: 'corr-state-1',
      causationId: 'req-state-1',
      createdBy: 'test-suite',
      source: 'TEST',
    });
    await paymentRepository.create(payment);

    let saved = await paymentRepository.findById(payment.id);
    expect(saved).not.toBeNull();
    expect(saved!.status).toBe(PaymentStatus.PENDING);

    saved!.transitionTo(PaymentStatus.PROCESSING);
    await paymentRepository.updateStatus(saved!.id, saved!.status);

    saved = await paymentRepository.findById(payment.id);
    expect(saved!.status).toBe(PaymentStatus.PROCESSING);

    saved!.transitionTo(PaymentStatus.COMPLETED);
    await paymentRepository.updateStatus(saved!.id, saved!.status);

    saved = await paymentRepository.findById(payment.id);
    expect(saved!.status).toBe(PaymentStatus.COMPLETED);
  });

  it('should throw InvalidPaymentStateTransitionException and leave database unchanged on invalid transition (COMPLETED -> PENDING)', async () => {
    const payment = PaymentEntity.create({
      merchantId: 'c0a52000-a000-4b0c-80a5-f00000000002',
      amount: 2500,
      currency: 'USD',
      reference: 'REF-STATE-2',
      requestId: 'req-state-2',
      correlationId: 'corr-state-2',
      causationId: 'req-state-2',
      createdBy: 'test-suite',
      source: 'TEST',
    });
    await paymentRepository.create(payment);

    let saved = await paymentRepository.findById(payment.id);
    saved!.transitionTo(PaymentStatus.PROCESSING);
    await paymentRepository.updateStatus(saved!.id, saved!.status);
    saved = await paymentRepository.findById(payment.id);
    saved!.transitionTo(PaymentStatus.COMPLETED);
    await paymentRepository.updateStatus(saved!.id, saved!.status);

    saved = await paymentRepository.findById(payment.id);
    expect(saved!.status).toBe(PaymentStatus.COMPLETED);

    expect(() => {
      saved!.transitionTo(PaymentStatus.PENDING);
    }).toThrow(InvalidPaymentStateTransitionException);

    const dbRecord = await prismaService.client.payment.findUnique({
      where: { id: payment.id },
    });
    expect(dbRecord!.status).toBe(PaymentStatus.COMPLETED);
  });
});
