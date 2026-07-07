import { registerAs } from '@nestjs/config';

import type { KafkaConfig } from '../types';

export default registerAs('kafka', (): KafkaConfig => ({
  brokers: process.env.KAFKA_BROKERS
    ? process.env.KAFKA_BROKERS.split(',').map((b) => b.trim())
    : ['localhost:29092'],
  clientId: process.env.KAFKA_CLIENT_ID || 'surgepay',
  ssl: process.env.KAFKA_SSL === 'true',
  sasl: process.env.KAFKA_SASL === 'true',
}));
