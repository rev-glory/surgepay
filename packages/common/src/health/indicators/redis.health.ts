import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { HealthCheckError, HealthIndicator, HealthIndicatorResult } from '@nestjs/terminus';
import { ConfigService } from '@surgepay/config';
import Redis from 'ioredis';

import { TIMEOUTS } from '../constants';

@Injectable()
export class RedisHealthIndicator extends HealthIndicator implements OnModuleDestroy {
  private redisClient: Redis | null = null;

  constructor(private readonly configService: ConfigService) {
    super();
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    try {
      const redisConfig = this.configService.redis;
      if (!redisConfig || !redisConfig.url) {
        throw new Error('Redis configuration is missing URL');
      }

      if (!this.redisClient) {
        this.redisClient = new Redis(redisConfig.url, {
          password: redisConfig.password || undefined,
          tls: redisConfig.tls ? {} : undefined,
          connectTimeout: TIMEOUTS.REDIS,
          maxRetriesPerRequest: 0,
          retryStrategy: () => null, // Prevents hanging retries on startup/disconnects
        });
      }

      const pong = await this.redisClient.ping();
      if (pong !== 'PONG') {
        throw new Error(`Redis ping returned unexpected result: ${pong}`);
      }

      return this.getStatus(key, true);
    } catch (error: any) {
      // Release client on connection failures to trigger recreation on next check
      this.cleanupClient();
      const result = this.getStatus(key, false, { message: error.message });
      throw new HealthCheckError('Redis health check failed', result);
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.cleanupClient();
  }

  private async cleanupClient(): Promise<void> {
    if (this.redisClient) {
      try {
        await this.redisClient.quit();
      } catch (_e) {
        try {
          this.redisClient.disconnect();
        } catch (_err) {}
      }
      this.redisClient = null;
    }
  }
}
