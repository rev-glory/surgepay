import { Injectable } from '@nestjs/common';
import { ConfigService as NestConfigService } from '@nestjs/config';

import {
  DatabaseConfig,
  HttpConfig,
  KafkaConfig,
  LoggingConfig,
  OutboxConfig,
  RedisConfig,
  SecurityConfig,
  ServicesConfig,
  TelemetryConfig,
} from './types';

@Injectable()
export class ConfigService {
  constructor(private readonly nestConfigService: NestConfigService) {}

  get database(): DatabaseConfig {
    return this.nestConfigService.getOrThrow<DatabaseConfig>('database');
  }

  get redis(): RedisConfig {
    return this.nestConfigService.getOrThrow<RedisConfig>('redis');
  }

  get kafka(): KafkaConfig {
    return this.nestConfigService.getOrThrow<KafkaConfig>('kafka');
  }

  get http(): HttpConfig {
    return this.nestConfigService.getOrThrow<HttpConfig>('http');
  }

  get logging(): LoggingConfig {
    return this.nestConfigService.getOrThrow<LoggingConfig>('logging');
  }

  get telemetry(): TelemetryConfig {
    return this.nestConfigService.getOrThrow<TelemetryConfig>('telemetry');
  }

  get security(): SecurityConfig {
    return this.nestConfigService.getOrThrow<SecurityConfig>('security');
  }

  get services(): ServicesConfig {
    return this.nestConfigService.getOrThrow<ServicesConfig>('services');
  }

  get outbox(): OutboxConfig {
    return this.nestConfigService.getOrThrow<OutboxConfig>('outbox');
  }
}
