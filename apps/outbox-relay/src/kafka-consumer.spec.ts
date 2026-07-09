import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { EventEnvelope } from '@surgepay/events';
import { LoggerService } from '@surgepay/common';
import { BaseKafkaConsumer, InboxPersister } from '@surgepay/common-messaging';

class TestConsumer extends BaseKafkaConsumer {
  public hookCalledWith: EventEnvelope | null = null;

  protected async onEventPersisted(envelope: EventEnvelope): Promise<void> {
    this.hookCalledWith = envelope;
  }
}

describe('BaseKafkaConsumer', () => {
  let mockKafka: jest.Mocked<Kafka>;
  let mockConsumer: jest.Mocked<Consumer>;
  let mockPersister: jest.Mocked<InboxPersister>;
  let mockLogger: jest.Mocked<LoggerService>;
  let testConsumer: TestConsumer;
  let runCallback: ((payload: EachMessagePayload) => Promise<void>) | null = null;

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
      persist: jest.fn().mockResolvedValue(undefined),
    };

    mockLogger = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    } as any;

    testConsumer = new TestConsumer(mockKafka, mockPersister, mockLogger, {
      groupId: 'test-group',
      topics: ['test-topic'],
    });
  });

  it('should cleanly connect, subscribe, and launch consumer run cycle', async () => {
    await testConsumer.connect();

    expect(mockConsumer.connect).toHaveBeenCalled();
    expect(mockConsumer.subscribe).toHaveBeenCalledWith(
      expect.objectContaining({ topic: 'test-topic', fromBeginning: true }),
    );
    expect(mockConsumer.run).toHaveBeenCalled();
  });

  it('should deserialize, validate, and persist valid events, then trigger hook', async () => {
    await testConsumer.connect();
    expect(runCallback).toBeDefined();

    const validPayload: EventEnvelope = {
      eventId: '11111111-1111-1111-1111-111111111111',
      eventType: 'PaymentInitiated',
      version: 1,
      timestamp: new Date().toISOString(),
      requestId: 'req-id',
      correlationId: '22222222-2222-2222-2222-222222222222',
      causationId: '33333333-3333-3333-3333-333333333333',
      sagaId: '44444444-4444-4444-4444-444444444444',
      producer: 'payment-service',
      payload: { amount: 100 },
    };

    const payloadBuffer = Buffer.from(JSON.stringify(validPayload));
    const mockEachMessagePayload: EachMessagePayload = {
      topic: 'test-topic',
      partition: 0,
      message: {
        key: null,
        value: payloadBuffer,
        timestamp: '0',
        attributes: 0,
        offset: '1',
        headers: {},
      },
      heartbeat: jest.fn().mockResolvedValue(undefined),
      pause: jest.fn().mockReturnValue(jest.fn()),
    };

    await runCallback!(mockEachMessagePayload);

    expect(mockPersister.persist).toHaveBeenCalledWith(
      expect.objectContaining({
        eventId: validPayload.eventId,
        eventType: validPayload.eventType,
      }),
    );
    expect(testConsumer.hookCalledWith).toEqual(
      expect.objectContaining({
        eventId: validPayload.eventId,
        eventType: validPayload.eventType,
      }),
    );
  });

  it('should log an error and throw if event validation fails', async () => {
    await testConsumer.connect();
    expect(runCallback).toBeDefined();

    // Missing eventId
    const malformedPayload = {
      eventType: 'PaymentInitiated',
      version: 1,
      timestamp: new Date().toISOString(),
    };

    const payloadBuffer = Buffer.from(JSON.stringify(malformedPayload));
    const mockEachMessagePayload: EachMessagePayload = {
      topic: 'test-topic',
      partition: 0,
      message: {
        key: null,
        value: payloadBuffer,
        timestamp: '0',
        attributes: 0,
        offset: '1',
        headers: {},
      },
      heartbeat: jest.fn().mockResolvedValue(undefined),
      pause: jest.fn().mockReturnValue(jest.fn()),
    };

    await expect(runCallback!(mockEachMessagePayload)).rejects.toThrow();
    expect(mockPersister.persist).not.toHaveBeenCalled();
    expect(mockLogger.error).toHaveBeenCalled();
  });
});
