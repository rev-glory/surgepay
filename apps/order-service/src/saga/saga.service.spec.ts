import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService } from '@surgepay/common';
import { PAYMENT_COMPLETED, type PaymentCompletedEvent } from '@surgepay/events';

import { SagaService } from './saga.service';

describe('SagaService', () => {
  let service: SagaService;
  let loggerMock: jest.Mocked<Partial<LoggerService>>;

  beforeEach(async () => {
    loggerMock = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SagaService,
        {
          provide: LoggerService,
          useValue: loggerMock,
        },
      ],
    }).compile();

    service = module.get<SagaService>(SagaService);
  });

  it('should initialize and set correct logger context', () => {
    expect(loggerMock.setContext).toHaveBeenCalledWith('SagaService');
  });

  it('should log payment completed event details at the entry point', async () => {
    const event: PaymentCompletedEvent = {
      eventId: 'evt_12345',
      eventType: PAYMENT_COMPLETED,
      correlationId: 'corr_54321',
      causationId: 'cause_99999',
      sagaId: 'saga_54321',
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

    await service.processPaymentCompleted(event);

    expect(loggerMock.info).toHaveBeenCalledWith(
      expect.stringContaining('Saga Orchestrator entry point reached for PaymentCompleted event'),
      expect.objectContaining({
        eventId: 'evt_12345',
        paymentId: 'pay_abc',
        correlationId: 'corr_54321',
        sagaId: 'saga_54321',
      }),
    );
  });
});
