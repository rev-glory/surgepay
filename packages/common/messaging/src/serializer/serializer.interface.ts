import type { BaseEventEnvelope } from '@surgepay/events';

export interface Serializer {
  serialize<T = any>(envelope: BaseEventEnvelope<T>): Buffer;
  deserialize<T = any>(data: Buffer): BaseEventEnvelope<T>;
}
