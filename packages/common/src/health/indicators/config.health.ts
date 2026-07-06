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
      // Access core configuration objects to verify they are loaded and not throwing errors
      const db = this.configService.database;
      const redis = this.configService.redis;
      const kafka = this.configService.kafka;

      if (!db || !redis || !kafka) {
        throw new Error('Config properties are undefined');
      }

      return this.getStatus(key, true);
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      const result = this.getStatus(key, false, { message: err.message });
      throw new HealthCheckError('Configuration check failed', result);
    }
  }
}
