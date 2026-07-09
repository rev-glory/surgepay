import { Injectable } from '@nestjs/common';
import { BaseEventEnvelope } from '@surgepay/events';

import { Serializer } from './serializer.interface';

@Injectable()
export class EventSerializer implements Serializer {
  serialize<T = any>(envelope: BaseEventEnvelope<T>): Buffer {
    return Buffer.from(JSON.stringify(envelope));
  }

  deserialize<T = any>(data: Buffer): BaseEventEnvelope<T> {
    return JSON.parse(data.toString()) as BaseEventEnvelope<T>;
  }
}
