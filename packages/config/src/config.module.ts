import * as fs from 'fs';
import * as path from 'path';

import { Module } from '@nestjs/common';
import { ConfigModule as NestConfigModule } from '@nestjs/config';

import { ConfigService } from './config.service';
import databaseConfig from './configs/database.config';
import httpConfig from './configs/http.config';
import kafkaConfig from './configs/kafka.config';
import loggingConfig from './configs/logging.config';
import outboxConfig from './configs/outbox.config';
import redisConfig from './configs/redis.config';
import sagaConfig from './configs/saga.config';
import securityConfig from './configs/security.config';
import servicesConfig from './configs/services.config';
import telemetryConfig from './configs/telemetry.config';
import { validate } from './validation';

function getEnvFilePath(): string {
  const env = process.env.NODE_ENV || 'development';
  const filename = `.env.${env}`;

  // Try current working directory
  let currentPath = path.resolve(process.cwd(), filename);
  if (fs.existsSync(currentPath)) {
    return currentPath;
  }

  // Try up to 3 parent directories (to locate workspace root)
  let dir = process.cwd();
  for (let i = 0; i < 3; i++) {
    dir = path.dirname(dir);
    currentPath = path.resolve(dir, filename);
    if (fs.existsSync(currentPath)) {
      return currentPath;
    }
  }

  return filename;
}

@Module({
  imports: [
    NestConfigModule.forRoot({
      isGlobal: true,
      envFilePath: getEnvFilePath(),
      load: [
        databaseConfig,
        redisConfig,
        kafkaConfig,
        httpConfig,
        loggingConfig,
        telemetryConfig,
        securityConfig,
        servicesConfig,
        outboxConfig,
        sagaConfig,
      ],
      validate,
    }),
  ],
  providers: [ConfigService],
  exports: [ConfigService],
})
export class ConfigModule {}
