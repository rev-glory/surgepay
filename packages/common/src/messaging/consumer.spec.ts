/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { propagation, trace } from '@opentelemetry/api';

import type { ConfigService } from '@surgepay/config';

import type { LoggerService } from '../logger';
import { BaseKafkaConsumer } from './consumer';
import type { BaseInboxRepository } from './inbox.repository';
import type { KafkaEventProducer } from './producer';
import { EventSerializer } from './serializer';

// Spies for consumer factory
const mockSubscribe = jest.fn();
const mockRun = jest.fn();
const mockConnect = jest.fn();
const mockDisconnect = jest.fn();
const mockCommitOffsets = jest.fn();

jest.mock('kafkajs', () => {
  return {
    Kafka: jest.fn().mockImplementation(() => {
      return {
        consumer: jest.fn().mockImplementation(() => {
          return {
            connect: mockConnect,
            disconnect: mockDisconnect,
            subscribe: mockSubscribe,
            run: mockRun,
            commitOffsets: mockCommitOffsets,
          };
        }),
      };
    }),
  };
});

class TestKafkaConsumer extends BaseKafkaConsumer {
  protected readonly topic = 'test-topic';
  protected readonly groupId = 'test-group';
  protected readonly inboxRepository: BaseInboxRepository;

  public handleEventMock = jest.fn();

  constructor(
    config: ConfigService,
    logger: LoggerService,
    producer: KafkaEventProducer,
    inboxRepository: BaseInboxRepository,
  ) {
    super(config, logger, producer);
    this.inboxRepository = inboxRepository;
  }

  protected async handleEvent(envelope: any): Promise<void> {
    await this.handleEventMock(envelope);
  }
}

describe('BaseKafkaConsumer Tracing Unit Tests', () => {
  let consumer: TestKafkaConsumer;
  let inboxRepositoryMock: jest.Mocked<BaseInboxRepository>;
  let producerMock: jest.Mocked<KafkaEventProducer>;
  let mockSpan: any;
  let mockTracer: any;

  const mockConfig = {
    kafka: {
      clientId: 'test-consumer-client',
      brokers: ['localhost:9092'],
      ssl: false,
      sasl: false,
      consumerRetryLimit: 3,
    },
    logging: {
      serviceName: 'test-service',
    },
  } as unknown as ConfigService;

  const mockLogger = {
    setContext: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  } as unknown as LoggerService;

  const mockEnvelope = {
    eventId: 'evt_123',
    eventType: 'PaymentInitiated',
    correlationId: 'corr_456',
    causationId: 'caus_789',
    sagaId: 'saga_abc',
    requestId: 'req_xyz',
    timestamp: '2026-07-13T12:00:00Z',
    version: 1,
    payload: { amount: 100 },
  };

  const serializedValue = EventSerializer.serialize(mockEnvelope);

  beforeEach(() => {
    jest.clearAllMocks();

    mockSpan = {
      end: jest.fn(),
      recordException: jest.fn(),
      setStatus: jest.fn(),
      setAttribute: jest.fn(),
      setAttributes: jest.fn(),
    };

    mockTracer = {
      startSpan: jest.fn().mockReturnValue(mockSpan),
    };

    jest.spyOn(trace, 'getTracer').mockReturnValue(mockTracer);
    jest.spyOn(propagation, 'extract').mockImplementation((ctx, carrier: any) => {
      return ctx;
    });

    inboxRepositoryMock = {
      findByEventIdAndConsumer: jest.fn(),
      recordReceived: jest.fn(),
      transitionStatus: jest.fn(),
      updateStatus: jest.fn(),
      countDlqDepth: jest.fn().mockResolvedValue(0),
    } as unknown as jest.Mocked<BaseInboxRepository>;

    producerMock = {
      publish: jest.fn(),
    } as unknown as jest.Mocked<KafkaEventProducer>;

    consumer = new TestKafkaConsumer(mockConfig, mockLogger, producerMock, inboxRepositoryMock);
  });

  it('should extract parent context, start a CONSUMER span, and execute handler under active context', async () => {
    inboxRepositoryMock.findByEventIdAndConsumer.mockResolvedValueOnce(null);
    inboxRepositoryMock.transitionStatus.mockResolvedValueOnce({
      id: 'evt_123',
      status: 'PROCESSING',
    } as any);

    // Retrieve eachMessage handler registered inside consumer.run
    await consumer.onModuleInit();
    const eachMessage = mockRun.mock.calls[0]?.[0]?.eachMessage;
    expect(eachMessage).toBeDefined();

    const headers = { traceparent: '00-mockparent-mockspan-01' };

    await eachMessage({
      topic: 'test-topic',
      partition: 0,
      message: {
        value: serializedValue,
        headers,
        offset: '10',
      },
    });

    // Assert trace context extraction occurred
    expect(propagation.extract).toHaveBeenCalledWith(expect.any(Object), headers, expect.any(Object));
    expect(mockTracer.startSpan).toHaveBeenCalledWith(
      'test-topic process',
      expect.objectContaining({
        kind: expect.any(Number), // SpanKind.CONSUMER is 4
        attributes: expect.objectContaining({
          'messaging.system': 'kafka',
          'messaging.destination.name': 'test-topic',
          'messaging.kafka.consumer.group': 'test-group',
        }),
      }),
      expect.any(Object),
    );

    // Assert consumer handler was called and span resolved successfully
    const { requestId, ...envelopeWithoutRequestId } = mockEnvelope;
    expect(consumer.handleEventMock).toHaveBeenCalledWith(
      expect.objectContaining(envelopeWithoutRequestId),
    );
    expect(mockSpan.setStatus).toHaveBeenCalledWith({ code: 1 }); // SpanStatusCode.OK
    expect(mockSpan.end).toHaveBeenCalled();
  });

  it('should annotate the span as a duplicate when duplicate events are skipped', async () => {
    // Mock existing as PROCESSED (duplicate)
    inboxRepositoryMock.findByEventIdAndConsumer.mockResolvedValueOnce({
      id: 'evt_123',
      status: 'PROCESSED',
      retryCount: 0,
    } as any);

    await consumer.onModuleInit();
    const eachMessage = mockRun.mock.calls[0]?.[0]?.eachMessage;

    await eachMessage({
      topic: 'test-topic',
      partition: 0,
      message: {
        value: serializedValue,
        offset: '10',
      },
    });

    // Verify duplication annotations on the span
    expect(mockSpan.setAttribute).toHaveBeenCalledWith('inbox.duplicate', true);
    expect(mockSpan.setAttribute).toHaveBeenCalledWith('inbox.status', 'PROCESSED');

    // Business handler must NOT run
    expect(consumer.handleEventMock).not.toHaveBeenCalled();
    expect(mockSpan.setStatus).toHaveBeenCalledWith({ code: 1 }); // SpanStatusCode.OK
    expect(mockSpan.end).toHaveBeenCalled();
  });

  it('should record handler exceptions on the span and re-throw them', async () => {
    inboxRepositoryMock.findByEventIdAndConsumer.mockResolvedValueOnce(null);
    inboxRepositoryMock.transitionStatus.mockResolvedValueOnce({
      id: 'evt_123',
      status: 'PROCESSING',
    } as any);
    inboxRepositoryMock.updateStatus.mockResolvedValueOnce({} as any);

    const handlerError = new Error('Database integrity crash');
    consumer.handleEventMock.mockRejectedValueOnce(handlerError);

    await consumer.onModuleInit();
    const eachMessage = mockRun.mock.calls[0]?.[0]?.eachMessage;

    await expect(
      eachMessage({
        topic: 'test-topic',
        partition: 0,
        message: {
          value: serializedValue,
          offset: '10',
        },
      }),
    ).rejects.toThrow(handlerError);

    // Span should capture the exception
    expect(mockSpan.recordException).toHaveBeenCalledWith(handlerError);
    expect(mockSpan.setStatus).toHaveBeenCalledWith({
      code: 2, // SpanStatusCode.ERROR
      message: 'Database integrity crash',
    });
    expect(mockSpan.end).toHaveBeenCalled();
  });

  it('should create DLQ publication child spans under the active consumer context', async () => {
    // Set mocks to force DLQ routing (retry limit exceeded)
    inboxRepositoryMock.findByEventIdAndConsumer.mockResolvedValue({
      id: 'evt_123',
      status: 'RETRYING',
      retryCount: 4, // exceeds consumerRetryLimit (3)
    } as any);
    inboxRepositoryMock.transitionStatus.mockResolvedValueOnce({
      id: 'evt_123',
      status: 'PROCESSING',
    } as any);

    const handlerError = new Error('Fatal exception');
    consumer.handleEventMock.mockRejectedValueOnce(handlerError);

    await consumer.onModuleInit();
    const eachMessage = mockRun.mock.calls[0]?.[0]?.eachMessage;

    await eachMessage({
      topic: 'test-topic',
      partition: 0,
      message: {
        value: serializedValue,
        offset: '10',
      },
    });

    // Verify DLQ publish operation was called
    expect(producerMock.publish).toHaveBeenCalledWith(
      'payments.dlq',
      'evt_123',
      expect.objectContaining({
        eventType: 'DeadLetterRecord',
      }),
    );
  });

  it('should run gracefully even when tracing headers are missing', async () => {
    inboxRepositoryMock.findByEventIdAndConsumer.mockResolvedValueOnce(null);
    inboxRepositoryMock.transitionStatus.mockResolvedValueOnce({
      id: 'evt_123',
      status: 'PROCESSING',
    } as any);

    await consumer.onModuleInit();
    const eachMessage = mockRun.mock.calls[0]?.[0]?.eachMessage;

    // No message.headers provided
    await eachMessage({
      topic: 'test-topic',
      partition: 0,
      message: {
        value: serializedValue,
        offset: '10',
      },
    });

    expect(consumer.handleEventMock).toHaveBeenCalled();
    expect(mockSpan.end).toHaveBeenCalled();
  });
});
