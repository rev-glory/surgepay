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

  it('should initialize and set correct logger context', () => {
    expect(loggerMock.setContext).toHaveBeenCalledWith('SagaService');
  });

  it('should create and persist a new SagaInstance when no duplicate is found', async () => {
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
      })
    );
    expect(loggerMock.info).toHaveBeenCalledWith(
      expect.stringContaining('Durable SagaInstance created successfully'),
      expect.objectContaining({
        sagaId: 'corr_54321',
        paymentId: 'pay_abc',
        correlationId: 'corr_54321',
      })
    );
  });

  it('should skip creating a SagaInstance if one already exists for the payment', async () => {
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

    const existingSagaMock = {
      id: 'existing_saga_id',
    } as unknown as SagaInstanceEntity;
    sagaRepositoryMock.findByPaymentId.mockResolvedValue(existingSagaMock);

    await service.processPaymentCompleted(event);

    expect(sagaRepositoryMock.findByPaymentId).toHaveBeenCalledWith('pay_abc');
    expect(sagaRepositoryMock.create).not.toHaveBeenCalled();
    expect(loggerMock.warn).toHaveBeenCalledWith(
      expect.stringContaining('SagaInstance already exists for payment'),
      expect.objectContaining({
        paymentId: 'pay_abc',
        sagaId: 'existing_saga_id',
        correlationId: 'corr_54321',
      })
    );
  });
});
