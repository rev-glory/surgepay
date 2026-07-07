import { Module } from '@nestjs/common';
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
        });
      },
      inject: [ConfigService],
    },
    RedisRateLimitRepository,
    RateLimitService,
  ],
  exports: [RateLimitService, RedisRateLimitRepository],
})
export class RateLimitModule {}
