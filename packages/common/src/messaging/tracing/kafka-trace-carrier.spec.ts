import { kafkaTextMapGetter, kafkaTextMapSetter } from './kafka-trace-carrier';

describe('Kafka Trace Carrier Spec', () => {
  describe('kafkaTextMapGetter', () => {
    it('should extract string values directly', () => {
      const carrier = {
        traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
      };
      const result = kafkaTextMapGetter.get(carrier, 'traceparent');
      expect(result).toBe('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01');
    });

    it('should convert and extract Buffer values to utf8 string', () => {
      const carrier = {
        traceparent: Buffer.from('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01', 'utf8'),
      };
      const result = kafkaTextMapGetter.get(carrier, 'traceparent');
      expect(result).toBe('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01');
    });

    it('should handle array values by mapping them to strings or converting Buffers', () => {
      const carrier = {
        traceparent: [
          Buffer.from('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01', 'utf8'),
          'some-other-value',
        ],
      };
      const result = kafkaTextMapGetter.get(carrier, 'traceparent');
      expect(result).toEqual([
        '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
        'some-other-value',
      ]);
    });

    it('should return undefined for missing or null keys', () => {
      const carrier = {};
      const result1 = kafkaTextMapGetter.get(carrier, 'traceparent');
      expect(result1).toBeUndefined();

      const carrierWithNull = { traceparent: null };
      const result2 = kafkaTextMapGetter.get(
        carrierWithNull as Record<string, unknown>,
        'traceparent',
      );
      expect(result2).toBeUndefined();
    });

    it('should return all keys from the carrier', () => {
      const carrier = {
        traceparent: 'foo',
        tracestate: 'bar',
      };
      const result = kafkaTextMapGetter.keys(carrier);
      expect(result).toEqual(['traceparent', 'tracestate']);
    });
  });

  describe('kafkaTextMapSetter', () => {
    it('should write key-value string pairs into the carrier', () => {
      const carrier: Record<string, string> = {};
      kafkaTextMapSetter.set(
        carrier,
        'traceparent',
        '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
      );
      expect(carrier.traceparent).toBe('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01');
    });
  });
});
