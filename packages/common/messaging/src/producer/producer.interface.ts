import { RecordMetadata } from 'kafkajs';
import { BaseEventEnvelope } from '@surgepay/events';

export interface IProducer {
  publish<T = any>(topic: string, event: BaseEventEnvelope<T>): Promise<RecordMetadata[]>;
  publishBatch(messages: Array<{ topic: string; event: BaseEventEnvelope<any> }>): Promise<RecordMetadata[]>;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  isReady(): boolean;
}
