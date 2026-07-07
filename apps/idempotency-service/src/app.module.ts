import { Module } from '@nestjs/common';
import { ConfigModule as NestConfigModule } from '@nestjs/config';
import { TerminusModule } from '@nestjs/terminus';

import { HealthModule, LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import idempotencyConfig from './config/idempotency.config';
import redisConfig from './config/redis.config';
import { IdempotencyModule } from './modules/idempotency.module';
import { RedisModule } from './modules/redis.module';

@Module({
  imports: [
    ConfigModule,
    NestConfigModule.forFeature(idempotencyConfig),
    NestConfigModule.forFeature(redisConfig),
    LoggerModule,
    TerminusModule,
    HealthModule,
    RedisModule,
    IdempotencyModule,
  ],
})
export class AppModule {}
