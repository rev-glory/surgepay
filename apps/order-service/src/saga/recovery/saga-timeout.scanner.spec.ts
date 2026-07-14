import { Test, type TestingModule } from '@nestjs/testing';
import { LoggerService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import { SagaTimeoutScanner } from './saga-timeout.scanner';
import { SagaRepository } from '../repositories/saga.repository';
import { OrderOutboxRepository } from '../repositories/order-outbox.repository';
import { SagaStatus } from '../../generated/client';

describe('SagaTimeoutScanner', () => {
  let scanner: SagaTimeoutScanner;
  let sagaRepository: jest.Mocked<SagaRepository>;
  let outboxRepository: jest.Mocked<OrderOutboxRepository>;

  const mockConfigService = {
    saga: {
      scanIntervalMs: 5000,
      stepTimeoutMs: 60000,
      maxRetryAttempts: 3,
      retryBaseDelayMs: 2000,
      retryMaxDelayMs: 10000,
      batchSize: 10,
      handoffTimeoutMs: 300000,
    },
  };

  const mockLoggerService = {
    setContext: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  };

  const mockSagaInstance = {
    id: 'test-saga-id',
    paymentId: 'test-payment-id',
    correlationId: 'test-saga-id',
    status: SagaStatus.LEDGER_PENDING,
    orderValidationStatus: 'CONFIRMED',
    merchantId: 'test-merchant',
    amount: 100,
    currency: 'USD',
    version: 1,
    retryCount: 0,
    currentCommandId: 'original-cmd-123',
    startHandoff: jest.fn(),
    retryHandoffAt: null,
  };

  const mockPrismaClient = {
    $transaction: jest.fn().mockImplementation(async (cb) => {
      const tx = {
        sagaInstance: {
          findUnique: jest.fn().mockResolvedValue(mockSagaInstance),
          update: jest.fn().mockResolvedValue(mockSagaInstance),
        },
        orderOutboxEvent: {
          create: jest.fn().mockResolvedValue({}),
        },
      };
      return cb(tx);
    }),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SagaTimeoutScanner,
        {
          provide: SagaRepository,
          useValue: {
            findStalledSagas: jest.fn(),
            update: jest.fn(),
            prisma: { client: mockPrismaClient },
          },
        },
        {
          provide: OrderOutboxRepository,
          useValue: {
            save: jest.fn(),
          },
        },
        {
          provide: ConfigService,
          useValue: mockConfigService,
        },
        {
          provide: LoggerService,
          useValue: mockLoggerService,
        },
      ],
    }).compile();

    scanner = module.get<SagaTimeoutScanner>(SagaTimeoutScanner);
    sagaRepository = module.get(SagaRepository);
    outboxRepository = module.get(OrderOutboxRepository);
  });

  it('should find stalled sagas and trigger handoff and schedule retry', async () => {
    sagaRepository.findStalledSagas.mockResolvedValue([mockSagaInstance as any]);

    await (scanner as any).scanForTimeouts();

    expect(sagaRepository.findStalledSagas).toHaveBeenCalled();
    expect(mockSagaInstance.startHandoff).toHaveBeenCalled();
    expect(mockPrismaClient.$transaction).toHaveBeenCalled();
  });
});
