import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@surgepay/config';
import { CompressionTypes, Kafka, Producer } from 'kafkajs';

import { LoggerService } from '../logger';

export interface EventProducer {
  publish(topic: string, key: string, value: Buffer): Promise<void>;
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

  async publish(topic: string, key: string, value: Buffer): Promise<void> {
    // raw publish call without logging event errors inside the producer
    await this.producer.send({
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
