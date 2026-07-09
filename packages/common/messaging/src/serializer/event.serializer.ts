import { Injectable } from '@nestjs/common';
import { BaseEventEnvelope, EventSerializer as SharedSerializer } from '@surgepay/events';

import { Serializer } from './serializer.interface';

@Injectable()
export class EventSerializer implements Serializer {
  private readonly sharedSerializer = new SharedSerializer();

  serialize<T = any>(envelope: BaseEventEnvelope<T>): Buffer {
    return this.sharedSerializer.serialize(envelope);
  }

  deserialize<T = any>(data: Buffer): BaseEventEnvelope<T> {
    return this.sharedSerializer.deserialize(data);
  }
}
