import {
  EventDeserializationException,
  EventSerializer,
  KafkaEventProducer,
  type LoggerService,
  MalformedEventEnvelopeException,
  MissingEventUpgradePathException,
  SerializationException,
  UnsupportedEventVersionException,
  upgradeVersion,
  VersionUpgradeRegistry,
} from '@surgepay/common';
import type { ConfigService } from '@surgepay/config';
import type { BaseEventEnvelope } from '@surgepay/events';

import { type OutboxEvent, OutboxStatus, type Prisma } from './generated/client';
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
    VersionUpgradeRegistry.clear();

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
      timestamp: '2026-07-13T12:00:00Z',
      version: 1,
      payload: { amount: 1000 },
    };

    it('successfully serializes a valid envelope', () => {
      const buffer = EventSerializer.serialize(validEnvelope);
      const deserialized = JSON.parse(buffer.toString()) as BaseEventEnvelope<unknown>;
      // Expected output strictly has only the Section 9.1 fields and preserves values
      expect(deserialized).toEqual(validEnvelope);
    });

    it('throws SerializationException if envelope is not a valid object', () => {
      expect(() =>
        EventSerializer.serialize(null as unknown as BaseEventEnvelope<unknown>),
      ).toThrow(SerializationException);
    });

    it('throws MalformedEventEnvelopeException on missing core metadata fields', () => {
      const invalid = { ...validEnvelope, eventId: '' };
      expect(() => EventSerializer.serialize(invalid)).toThrow(MalformedEventEnvelopeException);

      const missingType = { ...validEnvelope, eventType: undefined } as unknown as BaseEventEnvelope<unknown>;
      expect(() => EventSerializer.serialize(missingType)).toThrow(MalformedEventEnvelopeException);

      const missingPayload = { ...validEnvelope } as Record<string, unknown>;
      delete missingPayload.payload;
      expect(() =>
        EventSerializer.serialize(missingPayload as unknown as BaseEventEnvelope<unknown>),
      ).toThrow(MalformedEventEnvelopeException);
    });

    it('throws MalformedEventEnvelopeException on invalid timestamp formats', () => {
      const invalidTime = { ...validEnvelope, timestamp: 'not-a-date' };
      expect(() => EventSerializer.serialize(invalidTime)).toThrow(MalformedEventEnvelopeException);
    });

    describe('serialize version boundaries', () => {
      it('throws UnsupportedEventVersionException on invalid version type/range', () => {
        const floatVer = { ...validEnvelope, version: 1.5 };
        expect(() => EventSerializer.serialize(floatVer)).toThrow(UnsupportedEventVersionException);

        const negativeVer = { ...validEnvelope, version: -1 };
        expect(() => EventSerializer.serialize(negativeVer)).toThrow(UnsupportedEventVersionException);

        const zeroVer = { ...validEnvelope, version: 0 };
        expect(() => EventSerializer.serialize(zeroVer)).toThrow(UnsupportedEventVersionException);
      });

      it('throws UnsupportedEventVersionException on historical versions (< CURRENT_EVENT_VERSION)', () => {
        const futureVer = { ...validEnvelope, version: 2 };
        expect(() => EventSerializer.serialize(futureVer)).toThrow(UnsupportedEventVersionException);
      });
    });

    describe('deserialize and version upgrade boundaries', () => {
      it('successfully deserializes and validates a current version event', () => {
        const buffer = EventSerializer.serialize(validEnvelope);
        const deserialized = EventSerializer.deserialize(buffer);
        expect(deserialized).toEqual(validEnvelope);
      });

      it('throws EventDeserializationException on malformed JSON payload input', () => {
        const badBuffer = Buffer.from('{invalid-json}');
        expect(() => EventSerializer.deserialize(badBuffer)).toThrow(EventDeserializationException);
      });

      it('throws MalformedEventEnvelopeException if JSON is parsed as non-object', () => {
        const nonObjectBuffer = Buffer.from('12345');
        expect(() => EventSerializer.deserialize(nonObjectBuffer)).toThrow(MalformedEventEnvelopeException);
      });

      it('throws UnsupportedEventVersionException on invalid/zero/negative versions', () => {
        const badVersionJson = { ...validEnvelope, version: 0 };
        const buffer = Buffer.from(JSON.stringify(badVersionJson));
        expect(() => EventSerializer.deserialize(buffer)).toThrow(UnsupportedEventVersionException);
      });

      it('upgrades historical version step-by-step to CURRENT_EVENT_VERSION if path exists', () => {
        const historical: BaseEventEnvelope<unknown> = {
          ...validEnvelope,
          version: 1,
        };

        VersionUpgradeRegistry.register('PaymentInitiated', 1, (env) => ({
          ...env,
          version: 2,
          payload: { ...(env.payload as Record<string, unknown>), upgraded: true },
        }));

        const upgraded = upgradeVersion(historical, 2);
        expect(upgraded.version).toBe(2);
        expect((upgraded.payload as Record<string, unknown>).upgraded).toBe(true);
      });

      it('throws MissingEventUpgradePathException if historical version lacks complete upgrade path', () => {
        const historical: BaseEventEnvelope<unknown> = {
          ...validEnvelope,
          version: 1,
        };

        expect(() => upgradeVersion(historical, 2)).toThrow(MissingEventUpgradePathException);
      });
    });
  });

  describe('KafkaEventProducer', () => {
    const testEnvelope: BaseEventEnvelope<unknown> = {
      eventId: 'evt_p1',
      eventType: 'PaymentInitiated',
      correlationId: 'corr_p1',
      causationId: 'caus_p1',
      sagaId: 'saga_p1',
      timestamp: '2026-07-13T12:00:00Z',
      version: 1,
      payload: { value: 'data' },
    };

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

    it('publishes message with correct send configuration after delegating serialization', async () => {
      const producer = new KafkaEventProducer(config, logger);
      await producer.publish('test-topic', 'test-key', testEnvelope);

      expect(mockSend).toHaveBeenCalledTimes(1);
      const sentPayload = mockSend.mock.calls[0]?.[0];
      expect(sentPayload.topic).toBe('test-topic');
      expect(sentPayload.acks).toBe(-1);
      expect(sentPayload.compression).toBe(1); // CompressionTypes.GZIP
      expect(sentPayload.messages[0].key).toBe('test-key');

      const publishedBuffer = sentPayload.messages[0].value as Buffer;
      const parsedEnvelope = JSON.parse(publishedBuffer.toString());
      expect(parsedEnvelope).toEqual(testEnvelope);
    });

    it('propagates raw broker errors directly up', async () => {
      const brokerError = new Error('Broker disconnected');
      mockSend.mockRejectedValueOnce(brokerError);

      const producer = new KafkaEventProducer(config, logger);
      await expect(
        producer.publish('test-topic', 'test-key', testEnvelope),
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
      timestamp: '2026-07-13T12:00:00Z',
      version: 1,
      payload: { amount: 1000 },
    };

    const mockDbEvent = (payload: unknown): OutboxEvent => ({
      id: 'evt_db_1',
      aggregateId: 'payment_agg_123',
      aggregateType: 'Payment',
      eventType: 'PaymentInitiated',
      payload: payload as Prisma.JsonValue,
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
        mockDbEnvelope,
      );
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
