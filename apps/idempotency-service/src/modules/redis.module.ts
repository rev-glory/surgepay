import { Global, Module } from '@nestjs/common';
import Redis from 'ioredis';

import { ConfigModule, ConfigService } from '@surgepay/config';

import { RedisRepository } from '../repositories/redis.repository';

@Global()
@Module({
  imports: [ConfigModule],
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
    RedisRepository,
  ],
  exports: ['REDIS_CLIENT', RedisRepository],
})
export class RedisModule {}
