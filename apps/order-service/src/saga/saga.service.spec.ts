import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService } from '@surgepay/common';
import { PAYMENT_COMPLETED, type PaymentCompletedEvent } from '@surgepay/events';

import type { SagaInstanceEntity } from './entities/saga-instance.entity';
import { SagaRepository } from './repositories/saga.repository';
import { SagaService } from './saga.service';

describe('SagaService', () => {
  let service: SagaService;
  let loggerMock: jest.Mocked<Partial<LoggerService>>;
  let sagaRepositoryMock: {
    findByPaymentId: jest.Mock;
    create: jest.Mock;
  };

  beforeEach(async () => {
    loggerMock = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };
    sagaRepositoryMock = {
      findByPaymentId: jest.fn(),
      create: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SagaService,
        {
          provide: LoggerService,
          useValue: loggerMock,
        },
        {
          provide: SagaRepository,
          useValue: sagaRepositoryMock,
        },
      ],
    }).compile();

    service = module.get<SagaService>(SagaService);
  });

  const event: PaymentCompletedEvent = {
    eventId: 'evt_12345',
    eventType: PAYMENT_COMPLETED,
    correlationId: 'corr_54321',
    causationId: 'cause_99999',
    sagaId: 'corr_54321',
    timestamp: new Date().toISOString(),
    version: 1,
    payload: {
      paymentId: 'pay_abc',
      amount: 5000,
      currency: 'USD',
      merchantId: 'merch_xyz',
      orderId: 'ord_123',
      processorTransactionId: 'txn_processor',
      completedAt: new Date().toISOString(),
    },
  };

  it('should initialize and set correct logger context', () => {
    expect(loggerMock.setContext).toHaveBeenCalledWith('SagaService');
  });

  it('should create and persist a new SagaInstance when no duplicate is found (Test 4)', async () => {
    sagaRepositoryMock.findByPaymentId.mockResolvedValue(null);
    sagaRepositoryMock.create.mockResolvedValue(null as unknown as SagaInstanceEntity);

    await service.processPaymentCompleted(event);

    expect(sagaRepositoryMock.findByPaymentId).toHaveBeenCalledWith('pay_abc');
    expect(sagaRepositoryMock.create).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'corr_54321',
        paymentId: 'pay_abc',
        correlationId: 'corr_54321',
        status: 'LEDGER_PENDING',
      }),
      expect.any(Array)
    );
  });

  it('should skip creating a SagaInstance if one already exists for the payment (Test 5)', async () => {
    const existingSagaMock = {
      id: 'corr_54321',
      paymentId: 'pay_abc',
      correlationId: 'corr_54321',
    } as unknown as SagaInstanceEntity;
    sagaRepositoryMock.findByPaymentId.mockResolvedValue(existingSagaMock);

    await service.processPaymentCompleted(event);

    expect(sagaRepositoryMock.findByPaymentId).toHaveBeenCalledWith('pay_abc');
    expect(sagaRepositoryMock.create).not.toHaveBeenCalled();
    expect(loggerMock.warn).toHaveBeenCalledWith(
      expect.stringContaining('SagaInstance already exists for payment'),
      expect.objectContaining({
        paymentId: 'pay_abc',
        sagaId: 'corr_54321',
        correlationId: 'corr_54321',
      })
    );
  });

  it('should return successfully on crash/redelivery convergence if Saga exists in DB (Test 6)', async () => {
    const existingSagaMock = {
      id: 'corr_54321',
      paymentId: 'pay_abc',
      correlationId: 'corr_54321',
    } as unknown as SagaInstanceEntity;

    sagaRepositoryMock.findByPaymentId.mockResolvedValue(existingSagaMock);

    // This resolves cleanly so the calling BaseKafkaConsumer can transition status to PROCESSED
    await expect(service.processPaymentCompleted(event)).resolves.not.toThrow();
    expect(sagaRepositoryMock.create).not.toHaveBeenCalled();
  });

  it('should handle concurrent insert race unique constraint failures idempotently (Test 7)', async () => {
    sagaRepositoryMock.findByPaymentId.mockResolvedValueOnce(null);

    // Mock Prisma P2002 unique constraint failed error
    const prismaError = new Error('Unique constraint failed') as Error & {
      code: string;
      meta?: { target: string[] };
    };
    prismaError.code = 'P2002';
    prismaError.meta = { target: ['paymentId'] };
    sagaRepositoryMock.create.mockRejectedValue(prismaError);

    // Mock findByPaymentId call after error to return the successfully created concurrent saga
    const concurrentSaga = {
      id: 'corr_54321',
      paymentId: 'pay_abc',
      correlationId: 'corr_54321',
    } as unknown as SagaInstanceEntity;
    sagaRepositoryMock.findByPaymentId.mockResolvedValueOnce(concurrentSaga);

    await expect(service.processPaymentCompleted(event)).resolves.not.toThrow();
    expect(loggerMock.warn).toHaveBeenCalledWith(
      expect.stringContaining('Duplicate SagaInstance insert race detected. Safe idempotent skip.'),
      expect.objectContaining({
        paymentId: 'pay_abc',
        sagaId: 'corr_54321',
        correlationId: 'corr_54321',
      })
    );
  });

  it('should re-throw P2002 errors that are correlation invariant violations (Test 8)', async () => {
    sagaRepositoryMock.findByPaymentId.mockResolvedValue(null);

    // Mock Prisma P2002 unique constraint failed error targeting correlationId
    const prismaError = new Error('Unique constraint failed') as Error & {
      code: string;
      meta?: { target: string[] };
    };
    prismaError.code = 'P2002';
    prismaError.meta = { target: ['correlationId'] };
    sagaRepositoryMock.create.mockRejectedValue(prismaError);

    await expect(service.processPaymentCompleted(event)).rejects.toThrow(
      'Unique constraint failed'
    );
  });
});
