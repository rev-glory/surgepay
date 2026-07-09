import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { BaseEventEnvelope } from '@surgepay/events';
import { RecordMetadata } from 'kafkajs';

import { KafkaProducer } from './kafka.producer';
import { IProducer } from './producer.interface';

@Injectable()
export class ProducerService implements IProducer, OnModuleInit, OnModuleDestroy {
  constructor(@Inject(KafkaProducer) private readonly kafkaProducer: KafkaProducer) {}

  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.disconnect();
  }

  async connect(): Promise<void> {
    await this.kafkaProducer.connect();
  }

  async disconnect(): Promise<void> {
    await this.kafkaProducer.disconnect();
  }

  isReady(): boolean {
    return this.kafkaProducer.isReady();
  }

  async publish<T = any>(topic: string, event: BaseEventEnvelope<T>): Promise<RecordMetadata[]> {
    return this.kafkaProducer.publish(topic, event);
  }
}
