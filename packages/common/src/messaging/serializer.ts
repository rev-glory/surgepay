import { BaseEventEnvelope } from '@surgepay/events';

export class SerializationException extends Error {
  constructor(message: string, public readonly originalError?: unknown) {
    super(message);
    this.name = 'SerializationException';
  }
}

export class EventSerializer {
  static serialize(envelope: BaseEventEnvelope<unknown>): Buffer {
    try {
      if (!envelope || typeof envelope !== 'object') {
        throw new Error('Envelope must be a valid object');
      }

      return Buffer.from(JSON.stringify(envelope));
    } catch (err) {
      throw new SerializationException(
        `Failed to serialize event: ${err instanceof Error ? err.message : String(err)}`,
        err,
      );
    }
  }
}
