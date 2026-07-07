import { Injectable } from '@nestjs/common';
import { HealthCheckError, HealthIndicator, HealthIndicatorResult } from '@nestjs/terminus';

import { ConfigService } from '@surgepay/config';

@Injectable()
export class ConfigHealthIndicator extends HealthIndicator {
  constructor(private readonly configService: ConfigService) {
    super();
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    try {
      const serviceName = process.env.SERVICE_NAME || this.configService.logging?.serviceName;
      if (!serviceName) {
        throw new Error('SERVICE_NAME environment variable is missing');
      }

      const port = process.env.PORT;
      if (!port) {
        throw new Error('PORT environment variable is missing');
      }

      switch (serviceName) {
        case 'merchant-service': {
          const dbUrl = process.env.DATABASE_URL;
          if (!dbUrl) {
            throw new Error('DATABASE_URL environment variable is missing');
          }
          break;
        }
        case 'idempotency-service': {
          const redisUrl = process.env.REDIS_URL;
          if (!redisUrl) {
            throw new Error('REDIS_URL environment variable is missing');
          }
          break;
        }
        default:
          break;
      }

      return this.getStatus(key, true);
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      const result = this.getStatus(key, false, { message: err.message });
      throw new HealthCheckError('Configuration check failed', result);
    }
  }

}
