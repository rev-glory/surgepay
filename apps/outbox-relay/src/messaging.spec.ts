import {
  EventSerializer,
  KafkaEventProducer,
  LoggerService,
  SerializationException,
} from '@surgepay/common';
import type { ConfigService } from '@surgepay/config';
import type { BaseEventEnvelope } from '@surgepay/events';

import { type OutboxEvent,OutboxStatus } from './generated/client';
import {
  EnvelopeMismatchException,
  KafkaOutboxPublisher,
  OutboxPublicationException,
} from './publisher';

// Define Mock Spies
const mockConnect = jest.fn().mockResolvedValue(undefined);
const mockDisconnect = jest.fn().mockResolvedValue(undefined);
const mockSend = jest.fn().mockResolvedValue(undefined);
const mockProducerFactory = jest.fn().mockReturnValue({
  connect: mockConnect,
  disconnect: mockDisconnect,
  send: mockSend,
});

// Mock kafkajs
jest.mock('kafkajs', () => {
  return {
    Kafka: jest.fn().mockImplementation(() => ({
      producer: mockProducerFactory,
    })),
    CompressionTypes: {
      GZIP: 1,
    },
  };
});

describe('Shared Messaging & Kafka Publisher Spec', () => {
  let config: jest.Mocked<ConfigService>;
  let logger: jest.Mocked<LoggerService>;

  beforeEach(() => {
    jest.clearAllMocks();

    config = {
      kafka: {
        clientId: 'test-client',
        brokers: ['localhost:9092'],
        ssl: false,
        sasl: false,
      },
    } as unknown as jest.Mocked<ConfigService>;

    logger = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    } as unknown as jest.Mocked<LoggerService>;
  });

  describe('EventSerializer', () => {
    const validEnvelope: BaseEventEnvelope<unknown> = {
      eventId: 'evt_1',
      eventType: 'PaymentInitiated',
      correlationId: 'corr_1',
      causationId: 'caus_1',
      sagaId: 'saga_1',
      requestId: 'req_1',
      timestamp: '2026-07-13T12:00:00Z',
      version: 1,
      payload: { amount: 1000 },
    };

    it('successfully serializes a valid envelope', () => {
      const buffer = EventSerializer.serialize(validEnvelope);
      const deserialized = JSON.parse(buffer.toString()) as BaseEventEnvelope<unknown>;
      expect(deserialized).toEqual(validEnvelope);
    });

    it('throws SerializationException if envelope is not a valid object', () => {
      expect(() =>
        EventSerializer.serialize(null as unknown as BaseEventEnvelope<unknown>),
      ).toThrow(SerializationException);
    });
  });

  describe('KafkaEventProducer', () => {
    it('instantiates producer with correct configuration', () => {
      new KafkaEventProducer(config, logger);
      expect(mockProducerFactory).toHaveBeenCalledWith({
        idempotent: true,
        maxInFlightRequests: 1,
        retry: {
          retries: 5,
        },
      });
    });

    it('manages connection lifecycle with NestJS hooks', async () => {
      const producer = new KafkaEventProducer(config, logger);
      await producer.onModuleInit();
      expect(mockConnect).toHaveBeenCalledTimes(1);

      await producer.onModuleDestroy();
      expect(mockDisconnect).toHaveBeenCalledTimes(1);
    });

    it('publishes message with correct send configuration', async () => {
      const producer = new KafkaEventProducer(config, logger);
      const testBuffer = Buffer.from('test-data');
      await producer.publish('test-topic', 'test-key', testBuffer);

      expect(mockSend).toHaveBeenCalledWith({
        topic: 'test-topic',
        acks: -1,
        compression: 1, // CompressionTypes.GZIP
        messages: [
          {
            key: 'test-key',
            value: testBuffer,
          },
        ],
      });
    });

    it('propagates raw broker errors directly up', async () => {
      const brokerError = new Error('Broker disconnected');
      mockSend.mockRejectedValueOnce(brokerError);

      const producer = new KafkaEventProducer(config, logger);
      await expect(
        producer.publish('test-topic', 'test-key', Buffer.from('test')),
      ).rejects.toThrow(brokerError);
    });
  });

  describe('KafkaOutboxPublisher', () => {
    let mockProducer: jest.Mocked<KafkaEventProducer>;
    let publisher: KafkaOutboxPublisher;

    const mockDbEnvelope: BaseEventEnvelope<unknown> = {
      eventId: 'evt_db_1',
      eventType: 'PaymentInitiated',
      correlationId: 'corr_db_1',
      causationId: 'caus_db_1',
      sagaId: 'saga_db_1',
      requestId: 'req_db_1',
      timestamp: '2026-07-13T12:00:00Z',
      version: 1,
      payload: { amount: 1000 },
    };

    const mockDbEvent = (payload: unknown): OutboxEvent => ({
      id: 'evt_db_1',
      aggregateId: 'payment_agg_123',
      aggregateType: 'Payment',
      eventType: 'PaymentInitiated',
      payload: payload as any, // eslint-disable-line @typescript-eslint/no-explicit-any
      status: OutboxStatus.PENDING,
      requestId: 'req_db_1',
      correlationId: 'corr_db_1',
      causationId: 'caus_db_1',
      createdAt: new Date('2026-07-13T12:00:00Z'),
      publishedAt: null,
      retryCount: 0,
      partition: null,
      offset: null,
      lastAttemptAt: null,
    });

    beforeEach(() => {
      mockProducer = {
        publish: jest.fn().mockResolvedValue([
          { topicName: 'payments.initiated', partition: 2, offset: '1024' },
        ]),
      } as unknown as jest.Mocked<KafkaEventProducer>;

      publisher = new KafkaOutboxPublisher(mockProducer);
    });

    it('verifies valid envelope, resolves topic from registry, and delegates to producer', async () => {
      await publisher.publish(mockDbEvent(mockDbEnvelope));

      expect(mockProducer.publish).toHaveBeenCalledTimes(1);
      expect(mockProducer.publish).toHaveBeenCalledWith(
        'payments.initiated',
        'payment_agg_123',
        expect.any(Buffer),
      );

      const sentBuffer = mockProducer.publish.mock.calls[0]?.[2] as Buffer;
      const sentEnvelope = JSON.parse(sentBuffer.toString()) as BaseEventEnvelope<unknown>;
      expect(sentEnvelope).toEqual(mockDbEnvelope);
    });

    it('throws fatal EnvelopeMismatchException if envelope payload is not an object', async () => {
      const badEvent = mockDbEvent(null);

      await expect(publisher.publish(badEvent)).rejects.toThrow(EnvelopeMismatchException);
      expect(mockProducer.publish).not.toHaveBeenCalled();
    });

    it('decorates and re-throws errors from the Kafka producer', async () => {
      const brokerError = new Error('Network partition');
      mockProducer.publish.mockRejectedValueOnce(brokerError);

      const eventObj = mockDbEvent(mockDbEnvelope);

      await expect(publisher.publish(eventObj)).rejects.toThrow(OutboxPublicationException);

      try {
        await publisher.publish(eventObj);
      } catch (err: unknown) {
        const publicationError = err as OutboxPublicationException;
        expect(publicationError.name).toBe('OutboxPublicationException');
        expect(publicationError.context).toEqual({
          eventId: eventObj.id,
          eventType: eventObj.eventType,
          correlationId: eventObj.correlationId,
          topic: 'payments.initiated',
        });
        expect(publicationError.originalError).toBe(brokerError);
      }
    });

    it('throws OutboxPublicationException if topic registry mapping is missing', async () => {
      const unknownEvent = mockDbEvent({
        ...mockDbEnvelope,
        eventType: 'UnknownEvent',
      });
      unknownEvent.eventType = 'UnknownEvent';

      await expect(publisher.publish(unknownEvent)).rejects.toThrow(OutboxPublicationException);
      await expect(publisher.publish(unknownEvent)).rejects.toThrow(
        /No topic mapping found for event type: UnknownEvent/,
      );
    });
  });
});
