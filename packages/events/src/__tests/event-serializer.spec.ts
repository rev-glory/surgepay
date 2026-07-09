import { EventSerializer, EventValidationError } from '../EventSerializer';
import { EventEnvelope } from '../EventEnvelope';
import { EventVersionRegistry } from '../EventVersion';

describe('EventSerializer', () => {
  let registry: EventVersionRegistry;
  let serializer: EventSerializer;

  beforeEach(() => {
    registry = new EventVersionRegistry();
    serializer = new EventSerializer(registry);
  });

  const validEnvelope: EventEnvelope<{ amount: number; currency: string }> = {
    eventId: 'test-event-uuid',
    eventType: 'PaymentInitiated',
    version: 1,
    timestamp: new Date().toISOString(),
    requestId: 'test-req-id',
    correlationId: 'test-corr-id',
    causationId: 'test-caus-id',
    sagaId: 'test-saga-id',
    producer: 'payment-service',
    payload: {
      amount: 1000,
      currency: 'USD',
    },
  };

  it('should successfully serialize and deserialize a valid envelope losslessly', () => {
    const serialized = serializer.serialize(validEnvelope);
    expect(Buffer.isBuffer(serialized)).toBe(true);

    const deserialized = serializer.deserialize<{ amount: number; currency: string }>(serialized);
    expect(deserialized).toEqual(validEnvelope);
    expect(deserialized.payload.amount).toBe(1000);
    expect(deserialized.payload.currency).toBe('USD');
  });

  it('should reject serialization if a required field is missing', () => {
    const malformed = { ...validEnvelope } as any;
    delete malformed.eventId;

    expect(() => serializer.serialize(malformed)).toThrow(EventValidationError);
    expect(() => serializer.serialize(malformed)).toThrow('Missing required envelope field "eventId"');
  });

  it('should reject serialization if version is not a number', () => {
    const malformed = { ...validEnvelope, version: '1' } as any;

    expect(() => serializer.serialize(malformed)).toThrow(EventValidationError);
    expect(() => serializer.serialize(malformed)).toThrow('"version" must be a number');
  });

  it('should reject deserialization of empty/null data', () => {
    expect(() => serializer.deserialize(Buffer.alloc(0))).toThrow(EventValidationError);
    expect(() => serializer.deserialize(Buffer.alloc(0))).toThrow('Data buffer is empty or null');
  });

  it('should support the version registry and central upgrade framework logic', () => {
    // Register a mock payload upgrader from v1 to v2
    registry.registerUpgrader('PaymentInitiated', 1, (payload: any) => {
      return {
        ...payload,
        amountInCents: payload.amount, // upgrade logic: rename / transform
      };
    });

    const serialized = serializer.serialize(validEnvelope);

    // Deserialize expecting version 2
    const deserialized = serializer.deserialize<any>(serialized, 2);

    expect(deserialized.version).toBe(2);
    expect(deserialized.payload.amountInCents).toBe(1000);
  });

  it('should throw an error if no upgrader is registered for a target version upgrade', () => {
    const serialized = serializer.serialize(validEnvelope);

    expect(() => serializer.deserialize(serialized, 2)).toThrow(EventValidationError);
    expect(() => serializer.deserialize(serialized, 2)).toThrow(
      'No upgrader registered for event type "PaymentInitiated" from version 1',
    );
  });
});
