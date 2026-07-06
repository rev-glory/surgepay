import { Injectable } from '@nestjs/common';
import { HealthCheckError, HealthIndicator, HealthIndicatorResult } from '@nestjs/terminus';
import { Admin, Kafka } from 'kafkajs';

import { ConfigService } from '@surgepay/config';

import { TIMEOUTS } from '../constants';

@Injectable()
export class KafkaHealthIndicator extends HealthIndicator {
  private kafka: Kafka | null = null;

  constructor(private readonly configService: ConfigService) {
    super();
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    let admin: Admin | null = null;
    try {
      const kafkaConfig = this.configService.kafka;
      if (!kafkaConfig || !kafkaConfig.brokers || kafkaConfig.brokers.length === 0) {
        throw new Error('Kafka configuration is missing brokers');
      }

      if (!this.kafka) {
        this.kafka = new Kafka({
          clientId: `${kafkaConfig.clientId || 'surgepay'}-healthcheck`,
          brokers: kafkaConfig.brokers,
          ssl: kafkaConfig.ssl,
          sasl: kafkaConfig.sasl
            ? {
                mechanism: 'plain',
                username: process.env.KAFKA_SASL_USERNAME || '',
                password: process.env.KAFKA_SASL_PASSWORD || '',
              }
            : undefined,
          connectionTimeout: TIMEOUTS.KAFKA,
        });
      }

      admin = this.kafka.admin();
      await admin.connect();
      const cluster = await admin.describeCluster();
      if (!cluster || !cluster.brokers || cluster.brokers.length === 0) {
        throw new Error('No active brokers found in cluster');
      }

      return this.getStatus(key, true);
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      const result = this.getStatus(key, false, { message: err.message });
      throw new HealthCheckError('Kafka health check failed', result);
    } finally {
      if (admin) {
        try {
          await admin.disconnect();
        } catch (_e) {
          // Ignore disconnect errors in finally block
        }
      }
    }
  }
}
