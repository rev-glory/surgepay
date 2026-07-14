import { Injectable } from '@nestjs/common';
import { HealthIndicatorResult } from '@nestjs/terminus';

import { ConfigService } from '@surgepay/config';

import { LoggerService } from '../logger/logger.service';
import { HEALTH_STATUS } from './constants';
import { ConfigHealthIndicator } from './indicators/config.health';
import { DatabaseHealthIndicator } from './indicators/database.health';
import { KafkaHealthIndicator } from './indicators/kafka.health';
import { RedisHealthIndicator } from './indicators/redis.health';
import {
  HealthStatus,
  LivenessResponse,
  OverallHealthResponse,
  ReadinessResponse,
} from './interfaces/health-status.interface';

@Injectable()
export class HealthService {
  constructor(
    private readonly dbIndicator: DatabaseHealthIndicator,
    private readonly redisIndicator: RedisHealthIndicator,
    private readonly kafkaIndicator: KafkaHealthIndicator,
    private readonly configIndicator: ConfigHealthIndicator,
    private readonly configService: ConfigService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('HealthService');
  }

  async checkLiveness(): Promise<LivenessResponse> {
    return { status: HEALTH_STATUS.UP };
  }

  async checkReadiness(): Promise<ReadinessResponse> {
    const checks: Record<string, HealthStatus> = {
      application: HEALTH_STATUS.UP,
    };
    let overallStatus: HealthStatus = HEALTH_STATUS.UP;

    const serviceName =
      process.env.SERVICE_NAME || this.configService.logging?.serviceName || 'unknown-service';

    const SERVICE_DEPENDENCIES: Record<string, string[]> = {
      'merchant-service': ['database', 'configuration'],
      'idempotency-service': ['redis', 'configuration'],
    };

    const deps = SERVICE_DEPENDENCIES[serviceName] || ['configuration'];

    const indicators: Array<{ key: string; check: () => Promise<HealthIndicatorResult> }> = [];

    if (deps.includes('database') && this.dbIndicator.isConfigured()) {
      indicators.push({ key: 'database', check: () => this.dbIndicator.isHealthy('database') });
    }
    if (deps.includes('redis')) {
      indicators.push({ key: 'redis', check: () => this.redisIndicator.isHealthy('redis') });
    }
    if (deps.includes('kafka')) {
      indicators.push({ key: 'kafka', check: () => this.kafkaIndicator.isHealthy('kafka') });
    }
    if (deps.includes('configuration')) {
      indicators.push({
        key: 'configuration',
        check: () => this.configIndicator.isHealthy('configuration'),
      });
    }

    for (const { key, check } of indicators) {
      try {
        const result = await check();
        const indicatorResult = result[key];
        const status = (
          indicatorResult?.status === 'up' ? HEALTH_STATUS.UP : HEALTH_STATUS.DOWN
        ) as HealthStatus;

        checks[key] = status;

        if (status === HEALTH_STATUS.DOWN) {
          overallStatus = HEALTH_STATUS.DOWN;
        }
      } catch (error) {
        checks[key] = HEALTH_STATUS.DOWN;
        overallStatus = HEALTH_STATUS.DOWN;

        const err = error instanceof Error ? error : new Error(String(error));
        this.logger.error(`Health check failed for dependency: ${key}`, {
          error: err.message,
          stack: err.stack,
        });
      }
    }

    return {
      status: overallStatus,
      checks,
    };
  }

  async checkOverallHealth(): Promise<OverallHealthResponse> {
    const readiness = await this.checkReadiness();
    const serviceName =
      process.env.SERVICE_NAME || this.configService.logging?.serviceName || 'unknown-service';

    return {
      status: readiness.status,
      service: serviceName,
      timestamp: new Date().toISOString(),
      checks: readiness.checks,
    };
  }
}
