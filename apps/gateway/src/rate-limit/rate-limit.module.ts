import { Inject, Module, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';

import { LoggerModule } from '@surgepay/common';
import { ConfigModule, ConfigService } from '@surgepay/config';

import { RateLimitService } from './rate-limit.service';
import { RedisRateLimitRepository } from './redis-rate-limit.repository';

@Module({
  imports: [ConfigModule, LoggerModule],
  providers: [
    {
      provide: 'REDIS_CLIENT',
      useFactory: (configService: ConfigService) => {
        const redisConfig = configService.redis;
        return new Redis(redisConfig.url, {
          password: redisConfig.password || undefined,
          tls: redisConfig.tls ? {} : undefined,
          maxRetriesPerRequest: null,
        });
      },
      inject: [ConfigService],
    },
    RedisRateLimitRepository,
    RateLimitService,
  ],
  exports: [RateLimitService, RedisRateLimitRepository],
})
export class RateLimitModule implements OnModuleDestroy {
  constructor(@Inject('REDIS_CLIENT') private readonly redis: Redis) {}

  async onModuleDestroy(): Promise<void> {
    try {
      if (this.redis.status !== 'end') {
        await this.redis.quit();
      }
    } catch (_e) {
      // Already closed or failed
    }
  }
}
