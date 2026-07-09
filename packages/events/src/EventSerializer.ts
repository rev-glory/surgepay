import type { EventEnvelope } from './EventEnvelope';
import type { EventVersionRegistry } from './EventVersion';

export class EventValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EventValidationError';
  }
}

export class EventSerializer {
  constructor(private readonly registry?: EventVersionRegistry) {}

  /**
   * Serializes an event envelope into a Buffer.
   * Deterministically validates the envelope before serialization.
   */
  serialize<T = unknown>(envelope: EventEnvelope<T>): Buffer {
    this.validate(envelope);
    return Buffer.from(JSON.stringify(envelope));
  }

  /**
   * Deserializes a Buffer into an event envelope.
   * Performs validation and upgrades the schema if a targetVersion is specified.
   */
  deserialize<T = unknown>(data: Buffer, targetVersion?: number): EventEnvelope<T> {
    if (!data || data.length === 0) {
      throw new EventValidationError('Data buffer is empty or null');
    }
    const json = JSON.parse(data.toString());
    let envelope = json as EventEnvelope<unknown>;
    this.validate(envelope);

    if (targetVersion !== undefined && envelope.version < targetVersion) {
      envelope = this.upgradeVersion(envelope, targetVersion);
    }

    return envelope as EventEnvelope<T>;
  }

  /**
   * Deterministically validates event envelope fields.
   */
  validate(envelope: unknown): void {
    if (!envelope) {
      throw new EventValidationError('Event envelope is null or undefined');
    }
    const env = envelope as Record<string, unknown>;
    const requiredFields = ['eventId', 'eventType', 'payload', 'timestamp', 'version'];
    for (const field of requiredFields) {
      if (env[field] === undefined || env[field] === null) {
        throw new EventValidationError(`Missing required envelope field "${field}"`);
      }
    }
    if (typeof env.version !== 'number') {
      throw new EventValidationError('"version" must be a number');
    }
  }

  /**
   * Sequentially upgrades the schema version of the payload using registered registry upgraders.
   */
  upgradeVersion(envelope: EventEnvelope<unknown>, targetVersion: number): EventEnvelope<unknown> {
    const currentEnvelope = { ...envelope };
    while (currentEnvelope.version < targetVersion) {
      const upgrader = this.registry?.getUpgrader(currentEnvelope.eventType, currentEnvelope.version);
      if (!upgrader) {
        throw new EventValidationError(
          `No upgrader registered for event type "${currentEnvelope.eventType}" from version ${currentEnvelope.version}`,
        );
      }
      currentEnvelope.payload = upgrader(currentEnvelope.payload);
      currentEnvelope.version += 1;
    }
    return currentEnvelope;
  }
}
