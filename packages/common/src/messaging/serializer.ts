import type { BaseEventEnvelope } from '@surgepay/events';

export const CURRENT_EVENT_VERSION = 1;
export const SUPPORTED_EVENT_VERSIONS = [1] as const;

// Base exception for serialization failures (for backwards compatibility)
export class SerializationException extends Error {
  constructor(message: string, public readonly originalError?: unknown) {
    super(message);
    this.name = 'SerializationException';
  }
}

// Specific validation and schema evolution exceptions
export class MalformedEventEnvelopeException extends SerializationException {
  constructor(message: string) {
    super(message);
    this.name = 'MalformedEventEnvelopeException';
  }
}

export class UnsupportedEventVersionException extends SerializationException {
  constructor(message: string, public readonly version?: number) {
    super(message);
    this.name = 'UnsupportedEventVersionException';
  }
}

export class EventDeserializationException extends Error {
  constructor(message: string, public readonly originalError?: unknown) {
    super(message);
    this.name = 'EventDeserializationException';
  }
}

export class MissingEventUpgradePathException extends Error {
  constructor(
    message: string,
    public readonly eventId?: string,
    public readonly eventType?: string,
    public readonly version?: number,
  ) {
    super(message);
    this.name = 'MissingEventUpgradePathException';
  }
}

// Registry managing version upgrade migrations
export class VersionUpgradeRegistry {
  private static readonly upgrades = new Map<
    string,
    Map<number, (envelope: BaseEventEnvelope<unknown>) => BaseEventEnvelope<unknown>>
  >();

  static register(
    eventType: string,
    fromVersion: number,
    upgradeFn: (envelope: BaseEventEnvelope<unknown>) => BaseEventEnvelope<unknown>,
  ): void {
    if (!this.upgrades.has(eventType)) {
      this.upgrades.set(eventType, new Map());
    }
    this.upgrades.get(eventType)!.set(fromVersion, upgradeFn);
  }

  static getUpgrade(
    eventType: string,
    fromVersion: number,
  ): ((envelope: BaseEventEnvelope<unknown>) => BaseEventEnvelope<unknown>) | undefined {
    return this.upgrades.get(eventType)?.get(fromVersion);
  }

  static clear(): void {
    this.upgrades.clear();
  }
}

// Validate envelope presence and core types (historical versions are allowed during validation)
export function validateEnvelope(envelope: unknown): asserts envelope is BaseEventEnvelope<unknown> {
  if (!envelope || typeof envelope !== 'object') {
    throw new MalformedEventEnvelopeException('Envelope must be a valid object');
  }

  const env = envelope as Record<string, unknown>;

  if (typeof env.eventId !== 'string' || !env.eventId.trim()) {
    throw new MalformedEventEnvelopeException('eventId is missing or empty');
  }

  if (typeof env.eventType !== 'string' || !env.eventType.trim()) {
    throw new MalformedEventEnvelopeException('eventType is missing or empty');
  }

  if (typeof env.correlationId !== 'string' || !env.correlationId.trim()) {
    throw new MalformedEventEnvelopeException('correlationId is missing or empty');
  }

  if (typeof env.causationId !== 'string' || !env.causationId.trim()) {
    throw new MalformedEventEnvelopeException('causationId is missing or empty');
  }

  if (typeof env.sagaId !== 'string' || !env.sagaId.trim()) {
    throw new MalformedEventEnvelopeException('sagaId is missing or empty');
  }

  if (typeof env.timestamp !== 'string' || !env.timestamp.trim() || isNaN(Date.parse(env.timestamp))) {
    throw new MalformedEventEnvelopeException('timestamp is missing or invalid');
  }

  if (typeof env.version !== 'number' || isNaN(env.version)) {
    throw new UnsupportedEventVersionException('version is missing or invalid');
  }

  if (env.payload === undefined) {
    throw new MalformedEventEnvelopeException('payload property must exist');
  }
}

// Upgrades historical envelopes step-by-step
export function upgradeVersion(
  envelope: BaseEventEnvelope<unknown>,
  targetVersion: number,
): BaseEventEnvelope<unknown> {
  if (!Number.isInteger(envelope.version) || envelope.version <= 0) {
    throw new UnsupportedEventVersionException(`Invalid event version: ${envelope.version}`, envelope.version);
  }

  if (envelope.version > CURRENT_EVENT_VERSION) {
    throw new UnsupportedEventVersionException(
      `Unsupported future event version: ${envelope.version}`,
      envelope.version,
    );
  }

  if (envelope.version === targetVersion) {
    return envelope;
  }

  let current = envelope;
  while (current.version < targetVersion) {
    const upgradeFn = VersionUpgradeRegistry.getUpgrade(current.eventType, current.version);
    if (!upgradeFn) {
      throw new MissingEventUpgradePathException(
        `No upgrade path found for event type "${current.eventType}" from version ${current.version} to version ${current.version + 1}`,
        current.eventId,
        current.eventType,
        current.version,
      );
    }
    current = upgradeFn(current);
  }

  return current;
}

export class EventSerializer {
  static serialize(envelope: BaseEventEnvelope<unknown>): Buffer {
    try {
      validateEnvelope(envelope);

      if (!Number.isInteger(envelope.version) || envelope.version <= 0) {
        throw new UnsupportedEventVersionException(`Invalid event version: ${envelope.version}`, envelope.version);
      }

      if (envelope.version !== CURRENT_EVENT_VERSION) {
        throw new UnsupportedEventVersionException(
          `Producer is restricted to publishing current event version (${CURRENT_EVENT_VERSION}), but got version ${envelope.version}`,
          envelope.version,
        );
      }

      // Build canonical envelope to strip any non-envelope properties
      const canonicalEnvelope: BaseEventEnvelope<unknown> = {
        eventId: envelope.eventId,
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        causationId: envelope.causationId,
        sagaId: envelope.sagaId,
        timestamp: envelope.timestamp,
        version: envelope.version,
        payload: envelope.payload,
      };

      return Buffer.from(JSON.stringify(canonicalEnvelope));
    } catch (err) {
      if (err instanceof SerializationException) {
        throw err;
      }
      throw new SerializationException(
        `Failed to serialize event: ${err instanceof Error ? err.message : String(err)}`,
        err,
      );
    }
  }

  static deserialize(buffer: Buffer): BaseEventEnvelope<unknown> {
    let parsed: unknown;
    try {
      parsed = JSON.parse(buffer.toString());
    } catch (err) {
      throw new EventDeserializationException('Failed to parse event JSON representation', err);
    }

    if (!parsed || typeof parsed !== 'object') {
      throw new MalformedEventEnvelopeException('Parsed JSON value is not a valid object');
    }

    validateEnvelope(parsed);

    // Apply any upgrades to resolve historical event versions to CURRENT_EVENT_VERSION
    const upgraded = upgradeVersion(parsed, CURRENT_EVENT_VERSION);

    return upgraded;
  }
}
