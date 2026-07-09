import type { KafkaConfig } from 'kafkajs';

export interface KafkaProducerOptions {
  kafkaConfig: KafkaConfig;
  requestTimeout?: number;
  connectionTimeout?: number;
  retries?: number;
}
