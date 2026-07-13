import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { CompressionTypes, Kafka, Producer, RecordMetadata } from 'kafkajs';

import { ConfigService } from '@surgepay/config';
import { BaseEventEnvelope } from '@surgepay/events';

import { LoggerService } from '../logger';
import { EventSerializer } from './serializer';

export interface EventProducer {
  publish(topic: string, key: string, event: BaseEventEnvelope<unknown>): Promise<RecordMetadata[]>;
}

export const EVENT_PRODUCER = 'EVENT_PRODUCER';

@Injectable()
export class KafkaEventProducer implements EventProducer, OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly producer: Producer;

  constructor(
    private readonly config: ConfigService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('KafkaEventProducer');

    const kafkaConfig = this.config.kafka;

    this.kafka = new Kafka({
      clientId: kafkaConfig.clientId,
      brokers: kafkaConfig.brokers,
      ssl: kafkaConfig.ssl,
      sasl: kafkaConfig.sasl
        ? {
            mechanism: 'plain', // fallback sasl config mechanism
            username: process.env.KAFKA_SASL_USERNAME || '',
            password: process.env.KAFKA_SASL_PASSWORD || '',
          }
        : undefined,
    });

    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
      retry: {
        retries: 5,
      },
    });
  }

  async onModuleInit(): Promise<void> {
    this.logger.info('Connecting to Redpanda/Kafka broker...');
    await this.producer.connect();
    this.logger.info('Successfully connected to Redpanda/Kafka broker.');
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.info('Disconnecting from Redpanda/Kafka broker...');
    await this.producer.disconnect();
    this.logger.info('Successfully disconnected from Redpanda/Kafka broker.');
  }

  async publish(topic: string, key: string, event: BaseEventEnvelope<unknown>): Promise<RecordMetadata[]> {
    const value = EventSerializer.serialize(event);
    return await this.producer.send({
      topic,
      acks: -1,
      compression: CompressionTypes.GZIP,
      messages: [
        {
          key,
          value,
        },
      ],
    });
  }
}
