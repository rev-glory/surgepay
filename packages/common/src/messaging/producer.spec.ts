/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { propagation, trace } from '@opentelemetry/api';

import type { ConfigService } from '@surgepay/config';

import type { LoggerService } from '../logger';
import { KafkaEventProducer } from './producer';

// Mock kafkajs
const mockSend = jest.fn();
jest.mock('kafkajs', () => {
  return {
    Kafka: jest.fn().mockImplementation(() => {
      return {
        producer: jest.fn().mockImplementation(() => {
          return {
            connect: jest.fn().mockResolvedValue(undefined),
            disconnect: jest.fn().mockResolvedValue(undefined),
            send: mockSend,
          };
        }),
      };
    }),
    CompressionTypes: {
      GZIP: 1,
    },
  };
});

describe('KafkaEventProducer Tracing Unit Tests', () => {
  let producer: KafkaEventProducer;
  let mockSpan: any;
  let mockTracer: any;
  const mockConfig = {
    kafka: {
      clientId: 'test-producer-client',
      brokers: ['localhost:9092'],
      ssl: false,
      sasl: false,
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

  beforeEach(() => {
    jest.clearAllMocks();

    mockSpan = {
      end: jest.fn(),
      recordException: jest.fn(),
      setStatus: jest.fn(),
    };

    mockTracer = {
      startSpan: jest.fn().mockReturnValue(mockSpan),
    };

    jest.spyOn(trace, 'getTracer').mockReturnValue(mockTracer);
    jest.spyOn(propagation, 'inject').mockImplementation((ctx, carrier: any) => {
      carrier['traceparent'] = '00-mocktrace-mockspan-01';
    });
    jest.spyOn(propagation, 'extract').mockImplementation((ctx, carrier: any) => {
      return ctx;
    });

    producer = new KafkaEventProducer(mockConfig, mockLogger);
  });

  it('should start a PRODUCER span with appropriate attributes and inject W3C trace context', async () => {
    mockSend.mockResolvedValueOnce([{ topicName: 'test-topic', partition: 0, offset: '1' }]);

    await producer.publish('test-topic', 'payment_123', mockEnvelope);

    // Assert Tracer started a span correctly
    expect(trace.getTracer).toHaveBeenCalledWith('surgepay-messaging');
    expect(mockTracer.startSpan).toHaveBeenCalledWith(
      'test-topic send',
      expect.objectContaining({
        kind: expect.any(Number), // SpanKind.PRODUCER is 3
        attributes: expect.objectContaining({
          'messaging.system': 'kafka',
          'messaging.destination.name': 'test-topic',
          'messaging.message.id': 'evt_123',
          'messaging.correlation_id': 'corr_456',
          'messaging.causation_id': 'caus_789',
          'messaging.event_type': 'PaymentInitiated',
        }),
      }),
      expect.any(Object),
    );

    // Assert headers injection occurred
    expect(propagation.inject).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: 'test-topic',
        messages: expect.arrayContaining([
          expect.objectContaining({
            key: 'payment_123',
            headers: expect.objectContaining({
              correlationId: 'corr_456',
              causationId: 'caus_789',
              requestId: 'req_xyz',
              traceparent: '00-mocktrace-mockspan-01',
            }),
          }),
        ]),
      }),
    );

    // Assert span resolved successfully and closed
    expect(mockSpan.setStatus).toHaveBeenCalledWith({ code: 1 }); // SpanStatusCode.OK is 1
    expect(mockSpan.end).toHaveBeenCalled();
  });

  it('should propagate correlation properties mapped directly from the envelope', async () => {
    mockSend.mockResolvedValueOnce([{ topicName: 'test-topic', partition: 0 }]);

    await producer.publish('test-topic', 'payment_123', mockEnvelope);

    const sentPayload = mockSend.mock.calls[0]?.[0];
    const headers = sentPayload.messages[0].headers;
    expect(headers.correlationId).toBe('corr_456');
    expect(headers.causationId).toBe('caus_789');
    expect(headers.requestId).toBe('req_xyz');
  });

  it('should preserve and record broker errors on the span and re-throw them', async () => {
    const brokerError = new Error('Kafka Connection Timeout');
    mockSend.mockRejectedValueOnce(brokerError);

    await expect(producer.publish('test-topic', 'payment_123', mockEnvelope)).rejects.toThrow(
      brokerError,
    );

    expect(mockSpan.recordException).toHaveBeenCalledWith(brokerError);
    expect(mockSpan.setStatus).toHaveBeenCalledWith({
      code: 2, // SpanStatusCode.ERROR is 2
      message: 'Kafka Connection Timeout',
    });
    expect(mockSpan.end).toHaveBeenCalled();
  });

  it('should isolate tracing errors so that publishing is successful even if tracing setup fails', async () => {
    mockSend.mockResolvedValueOnce([{ topicName: 'test-topic', partition: 0, offset: '2' }]);
    jest.spyOn(trace, 'getTracer').mockImplementationOnce(() => {
      throw new Error('Trace API crashed');
    });

    const result = await producer.publish('test-topic', 'payment_123', mockEnvelope);

    // Publishing should succeed despite tracing crash
    expect(result).toBeDefined();
    expect(mockSend).toHaveBeenCalled();
    expect(mockLogger.warn).toHaveBeenCalledWith(
      'Telemetry tracing initialization failed, degrading gracefully',
      { error: 'Trace API crashed' },
    );
  });
});
