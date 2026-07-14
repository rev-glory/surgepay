import { Test, type TestingModule } from '@nestjs/testing';
import { KafkaEventProducer, LoggerService } from '@surgepay/common';
import { RetrySchedulerService } from './retry-scheduler.service';
import { RetryRepository } from '../repositories/retry.repository';
import { createHash } from 'crypto';

describe('RetrySchedulerService', () => {
  let service: RetrySchedulerService;
  let retryRepository: jest.Mocked<RetryRepository>;
  let eventProducer: jest.Mocked<KafkaEventProducer>;

  const mockLoggerService = {
    setContext: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RetrySchedulerService,
        {
          provide: RetryRepository,
          useValue: {
            save: jest.fn(),
            findDue: jest.fn(),
            markExecuted: jest.fn(),
          },
        },
        {
          provide: KafkaEventProducer,
          useValue: {
            publish: jest.fn(),
          },
        },
        {
          provide: LoggerService,
          useValue: mockLoggerService,
        },
      ],
    }).compile();

    service = module.get<RetrySchedulerService>(RetrySchedulerService);
    retryRepository = module.get(RetryRepository);
    eventProducer = module.get(KafkaEventProducer);
  });

  it('should calculate deterministic ID and schedule retry under limits', async () => {
    const payload = {
      originalTopic: 'test.commands',
      originalEvent: {
        eventId: 'evt-123',
        correlationId: 'corr-123',
        sagaId: 'saga-123',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {},
      },
      retryCount: 0,
      maxAttempts: 3,
      baseDelayMs: 1000,
      maxDelayMs: 5000,
    };

    const expectedDeterministicId = createHash('sha256')
      .update('evt-123_1')
      .digest('hex');

    retryRepository.save.mockResolvedValue({} as any);

    await service.schedule(payload);

    expect(retryRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({
        id: expectedDeterministicId,
        retryCount: 1,
      })
    );
    expect(eventProducer.publish).toHaveBeenCalledWith(
      'retry.events',
      'saga-123',
      expect.objectContaining({
        eventType: 'SagaRetryRegistered',
      })
    );
  });

  it('should publish to DLQ and SagaStepExecutionFailed when attempts are exhausted', async () => {
    const payload = {
      originalTopic: 'test.commands',
      originalEvent: {
        eventId: 'evt-123',
        correlationId: 'corr-123',
        sagaId: 'saga-123',
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {},
      },
      retryCount: 3,
      maxAttempts: 3,
      baseDelayMs: 1000,
      maxDelayMs: 5000,
    };

    await service.schedule(payload);

    expect(retryRepository.save).not.toHaveBeenCalled();
    expect(eventProducer.publish).toHaveBeenCalledWith(
      'payments.dlq',
      'saga-123',
      expect.objectContaining({
        eventType: 'DeadLetterRecord',
      })
    );
    expect(eventProducer.publish).toHaveBeenCalledWith(
      'retry.events',
      'saga-123',
      expect.objectContaining({
        eventType: 'SagaStepExecutionFailed',
      })
    );
  });
});
