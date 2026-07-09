import { Kafka, type Producer } from 'kafkajs';

import type { KafkaProducerOptions } from './producer.options';

export function createKafkaProducer(options: KafkaProducerOptions): Producer {
  const kafka = new Kafka({
    clientId: options.kafkaConfig.clientId,
    brokers: options.kafkaConfig.brokers,
    ssl: options.kafkaConfig.ssl,
    sasl: options.kafkaConfig.sasl,
    connectionTimeout: options.connectionTimeout ?? 10000,
    requestTimeout: options.requestTimeout ?? 30000,
    retry: {
      initialRetryTime: 300,
      retries: options.retries ?? 5,
    },
  });

  return kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
  });
}
