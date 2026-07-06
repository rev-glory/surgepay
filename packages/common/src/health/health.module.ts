import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { ConfigModule } from '@surgepay/config';

import { LoggerModule } from '../logger/logger.module';
import { HealthController } from './health.controller';
import { HealthService } from './health.service';
import { ConfigHealthIndicator } from './indicators/config.health';
import { DatabaseHealthIndicator } from './indicators/database.health';
import { KafkaHealthIndicator } from './indicators/kafka.health';
import { RedisHealthIndicator } from './indicators/redis.health';

@Module({
  imports: [TerminusModule, ConfigModule, LoggerModule],
  controllers: [HealthController],
  providers: [
    HealthService,
    ConfigHealthIndicator,
    DatabaseHealthIndicator,
    KafkaHealthIndicator,
    RedisHealthIndicator,
  ],
  exports: [
    HealthService,
    ConfigHealthIndicator,
    DatabaseHealthIndicator,
    KafkaHealthIndicator,
    RedisHealthIndicator,
  ],
})
export class HealthModule {}
