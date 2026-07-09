import type { Consumer, EachMessagePayload,Kafka } from 'kafkajs';

import type { LoggerService, MetricsService } from '@surgepay/common';
import { type EventEnvelope, type InboxEvent,InboxStatus } from '@surgepay/events';

import type { DlqPublisher } from './dlq.publisher';
import { DuplicateEventException, EventCurrentlyProcessingException } from './duplicate-event.exception';
import type { KafkaEventHandler } from './event-handler.interface';
import { BaseKafkaConsumer, type InboxPersister } from './kafka-consumer';

class DummyConsumer extends BaseKafkaConsumer {
  protected async onEventPersisted(_envelope: EventEnvelope): Promise<void> {}
}

describe('Idempotent Kafka Consumer', () => {
  let mockKafka: jest.Mocked<Kafka>;
  let mockConsumer: jest.Mocked<Consumer>;
  let mockPersister: jest.Mocked<InboxPersister>;
  let mockHandler: jest.Mocked<KafkaEventHandler>;
  let mockDlqPublisher: jest.Mocked<DlqPublisher>;
  let mockLogger: jest.Mocked<LoggerService>;
  let mockMetricsService: jest.Mocked<MetricsService>;
  let consumerInstance: DummyConsumer;
  let runCallback: ((payload: EachMessagePayload) => Promise<void>) | null = null;

  const validEnvelope: EventEnvelope = {
    eventId: '55555555-5555-5555-5555-555555555555',
    eventType: 'PaymentInitiated',
    version: 1,
    timestamp: new Date().toISOString(),
    requestId: 'req-123',
    correlationId: '66666666-6666-6666-6666-666666666666',
    causationId: '77777777-7777-7777-7777-777777777777',
    sagaId: '88888888-8888-8888-8888-888888888888',
    producer: 'payment-service',
    payload: { amount: 200 },
  };

  const payloadBuffer = Buffer.from(JSON.stringify(validEnvelope));

  const mockEachMessagePayload: EachMessagePayload = {
    topic: 'payment.events',
    partition: 0,
    message: {
      key: null,
      value: payloadBuffer,
      timestamp: '0',
      attributes: 0,
      offset: '42',
      headers: {},
    },
    heartbeat: jest.fn().mockResolvedValue(undefined),
    pause: jest.fn().mockReturnValue(jest.fn()),
  };

  beforeEach(() => {
    mockConsumer = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockImplementation((options) => {
        runCallback = options.eachMessage;
        return Promise.resolve();
      }),
    } as any;

    mockKafka = {
      consumer: jest.fn().mockReturnValue(mockConsumer),
    } as any;

    mockPersister = {
      find: jest.fn(),
      persistReceived: jest.fn(),
      markProcessing: jest.fn(),
      markProcessed: jest.fn(),
      markFailed: jest.fn(),
      markRetrying: jest.fn(),
      markDlqSent: jest.fn(),
    };

    mockHandler = {
      handle: jest.fn().mockResolvedValue(undefined),
    };

    mockDlqPublisher = {
      publish: jest.fn().mockResolvedValue(undefined),
    };

    mockLogger = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    } as any;

    mockMetricsService = {
      incrementPublished: jest.fn(),
      recordPublishDuration: jest.fn(),
      incrementPublishFailures: jest.fn(),
      incrementRetries: jest.fn(),
      incrementConsumed: jest.fn(),
      recordConsumerDuration: jest.fn(),
      incrementDuplicates: jest.fn(),
      incrementConsumerFailures: jest.fn(),
      setPendingEvents: jest.fn(),
      setPublishedEvents: jest.fn(),
      setFailedEvents: jest.fn(),
      incrementReceived: jest.fn(),
      incrementProcessed: jest.fn(),
      incrementDlqEvents: jest.fn(),
    } as any;

    consumerInstance = new DummyConsumer(
      mockKafka,
      mockPersister,
      mockHandler,
      mockDlqPublisher,
      mockLogger,
      mockMetricsService,
      {
        groupId: 'test-group',
        topics: ['payment.events'],
        dlqTopic: 'payment.dlq',
        maxRetries: 3,
      },
    );
  });

  it('Scenario 1: First Delivery - persists and executes successfully', async () => {
    const mockRecord: InboxEvent = {
      id: 'db-id-1',
      eventId: validEnvelope.eventId,
      consumer: 'test-group',
      eventType: validEnvelope.eventType,
      status: InboxStatus.RECEIVED,
      payload: validEnvelope.payload,
      correlationId: validEnvelope.correlationId,
      causationId: validEnvelope.causationId,
      sagaId: validEnvelope.sagaId,
      receivedAt: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      retryCount: 0,
    };

    mockPersister.persistReceived.mockResolvedValue(mockRecord);

    await consumerInstance.connect();
    await runCallback!(mockEachMessagePayload);

    expect(mockPersister.persistReceived).toHaveBeenCalledWith(validEnvelope);
    expect(mockPersister.markProcessing).toHaveBeenCalledWith('db-id-1');
    expect(mockHandler.handle).toHaveBeenCalledWith(validEnvelope);
    expect(mockPersister.markProcessed).toHaveBeenCalledWith('db-id-1');
    expect(mockMetricsService.incrementConsumed).toHaveBeenCalledWith('test-group', 'PaymentInitiated', 'test-group');
    expect(mockMetricsService.incrementReceived).toHaveBeenCalledWith('test-group', 'PaymentInitiated', 'test-group');
    expect(mockMetricsService.incrementProcessed).toHaveBeenCalledWith('test-group', 'PaymentInitiated', 'test-group');
    expect(mockMetricsService.recordConsumerDuration).toHaveBeenCalled();
  });

  it('Scenario 2: Duplicate Delivery - skips handler on PROCESSED events', async () => {
    mockPersister.persistReceived.mockRejectedValue(
      new DuplicateEventException(validEnvelope.eventId, 'test-group'),
    );

    const mockExistingRecord: InboxEvent = {
      id: 'db-id-1',
      eventId: validEnvelope.eventId,
      consumer: 'test-group',
      eventType: validEnvelope.eventType,
      status: InboxStatus.PROCESSED,
      payload: validEnvelope.payload,
      correlationId: validEnvelope.correlationId,
      causationId: validEnvelope.causationId,
      sagaId: validEnvelope.sagaId,
      receivedAt: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      retryCount: 0,
    };

    mockPersister.find.mockResolvedValue(mockExistingRecord);

    await consumerInstance.connect();
    await runCallback!(mockEachMessagePayload);

    expect(mockPersister.persistReceived).toHaveBeenCalled();
    expect(mockPersister.find).toHaveBeenCalledWith('test-group', validEnvelope.eventId);
    expect(mockHandler.handle).not.toHaveBeenCalled();
    expect(mockLogger.info).toHaveBeenCalledWith(
      'Duplicate event detected (already processed/DLQed). Skipping business logic.',
      expect.objectContaining({ duplicate: true }),
    );
    expect(mockMetricsService.incrementDuplicates).toHaveBeenCalledWith('test-group', 'PaymentInitiated', 'test-group');
  });

  it('Scenario 3: Concurrent Duplicates - database constraint triggers skip', async () => {
    mockPersister.persistReceived.mockRejectedValue(
      new DuplicateEventException(validEnvelope.eventId, 'test-group'),
    );

    const mockExistingRecord: InboxEvent = {
      id: 'db-id-1',
      eventId: validEnvelope.eventId,
      consumer: 'test-group',
      eventType: validEnvelope.eventType,
      status: InboxStatus.PROCESSED,
      payload: validEnvelope.payload,
      correlationId: validEnvelope.correlationId,
      causationId: validEnvelope.causationId,
      sagaId: validEnvelope.sagaId,
      receivedAt: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      retryCount: 0,
    };

    mockPersister.find.mockResolvedValue(mockExistingRecord);

    await consumerInstance.connect();
    await runCallback!(mockEachMessagePayload);

    expect(mockPersister.persistReceived).toHaveBeenCalled();
    expect(mockPersister.find).toHaveBeenCalledWith('test-group', validEnvelope.eventId);
    expect(mockHandler.handle).not.toHaveBeenCalled();
  });

  it('Scenario 4: Existing Inbox Record Recovery - retry states re-execute', async () => {
    mockPersister.persistReceived.mockRejectedValue(
      new DuplicateEventException(validEnvelope.eventId, 'test-group'),
    );

    const mockExistingFailedRecord: InboxEvent = {
      id: 'db-id-1',
      eventId: validEnvelope.eventId,
      consumer: 'test-group',
      eventType: validEnvelope.eventType,
      status: InboxStatus.FAILED,
      payload: validEnvelope.payload,
      correlationId: validEnvelope.correlationId,
      causationId: validEnvelope.causationId,
      sagaId: validEnvelope.sagaId,
      receivedAt: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      retryCount: 1,
    };

    mockPersister.find.mockResolvedValue(mockExistingFailedRecord);

    await consumerInstance.connect();
    await runCallback!(mockEachMessagePayload);

    expect(mockPersister.find).toHaveBeenCalledWith('test-group', validEnvelope.eventId);
    expect(mockPersister.markProcessing).toHaveBeenCalledWith('db-id-1');
    expect(mockHandler.handle).toHaveBeenCalledWith(validEnvelope);
    expect(mockPersister.markProcessed).toHaveBeenCalledWith('db-id-1');
  });

  it('Scenario 5: Concurrent Processing Block - throws EventCurrentlyProcessingException', async () => {
    mockPersister.persistReceived.mockRejectedValue(
      new DuplicateEventException(validEnvelope.eventId, 'test-group'),
    );

    const mockExistingProcessingRecord: InboxEvent = {
      id: 'db-id-1',
      eventId: validEnvelope.eventId,
      consumer: 'test-group',
      eventType: validEnvelope.eventType,
      status: InboxStatus.PROCESSING,
      payload: validEnvelope.payload,
      correlationId: validEnvelope.correlationId,
      causationId: validEnvelope.causationId,
      sagaId: validEnvelope.sagaId,
      receivedAt: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      retryCount: 0,
    };

    mockPersister.find.mockResolvedValue(mockExistingProcessingRecord);

    await consumerInstance.connect();

    await expect(runCallback!(mockEachMessagePayload)).rejects.toThrow(
      EventCurrentlyProcessingException,
    );

    expect(mockPersister.find).toHaveBeenCalledWith('test-group', validEnvelope.eventId);
    expect(mockHandler.handle).not.toHaveBeenCalled();
  });

  it('Scenario 6: Retry Limit Under Maximum - marks failed and retrying, throws error', async () => {
    const mockRecord: InboxEvent = {
      id: 'db-id-1',
      eventId: validEnvelope.eventId,
      consumer: 'test-group',
      eventType: validEnvelope.eventType,
      status: InboxStatus.RECEIVED,
      payload: validEnvelope.payload,
      correlationId: validEnvelope.correlationId,
      causationId: validEnvelope.causationId,
      sagaId: validEnvelope.sagaId,
      receivedAt: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      retryCount: 1,
    };

    mockPersister.persistReceived.mockResolvedValue(mockRecord);
    mockHandler.handle.mockRejectedValue(new Error('Transient database deadlock'));

    await consumerInstance.connect();
    await expect(runCallback!(mockEachMessagePayload)).rejects.toThrow('Transient database deadlock');

    expect(mockPersister.markProcessing).toHaveBeenCalledWith('db-id-1');
    expect(mockPersister.markFailed).toHaveBeenCalledWith('db-id-1', 'Transient database deadlock');
    expect(mockPersister.markRetrying).toHaveBeenCalledWith('db-id-1', 'Transient database deadlock');
    expect(mockMetricsService.incrementConsumerFailures).toHaveBeenCalledWith('test-group', 'PaymentInitiated', 'test-group');
  });

  it('Scenario 7: Retry Limit Exhausted - wraps and publishes to DLQ, resolves cleanly', async () => {
    const mockRecord: InboxEvent = {
      id: 'db-id-1',
      eventId: validEnvelope.eventId,
      consumer: 'test-group',
      eventType: validEnvelope.eventType,
      status: InboxStatus.RECEIVED,
      payload: validEnvelope.payload,
      correlationId: validEnvelope.correlationId,
      causationId: validEnvelope.causationId,
      sagaId: validEnvelope.sagaId,
      receivedAt: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      retryCount: 3,
    };

    mockPersister.persistReceived.mockResolvedValue(mockRecord);
    mockHandler.handle.mockRejectedValue(new Error('Permanent deserialization error'));

    await consumerInstance.connect();
    await runCallback!(mockEachMessagePayload); // resolves cleanly, offset committed

    expect(mockPersister.markFailed).toHaveBeenCalledWith('db-id-1', 'Permanent deserialization error');
    expect(mockMetricsService.incrementConsumerFailures).toHaveBeenCalledWith('test-group', 'PaymentInitiated', 'test-group');
    expect(mockDlqPublisher.publish).toHaveBeenCalledWith(
      'payment.dlq',
      expect.objectContaining({
        eventType: 'DeadLetterEvent',
        correlationId: validEnvelope.correlationId,
        sagaId: validEnvelope.sagaId,
        payload: expect.objectContaining({
          consumer: 'test-group',
          retryCount: 3,
          failureReason: 'Permanent deserialization error',
        }),
      }),
    );
    expect(mockPersister.markDlqSent).toHaveBeenCalledWith('db-id-1');
    expect(mockMetricsService.incrementDlqEvents).toHaveBeenCalledWith('test-group', 'PaymentInitiated', 'test-group');
  });
});
