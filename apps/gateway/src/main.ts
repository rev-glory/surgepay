import * as fs from 'fs';
import * as path from 'path';

import { VersioningType } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import * as dotenv from 'dotenv';

// Manual bootstrap phase to load the environment variables from the correct workspace level env file
const env = process.env.NODE_ENV || 'development';
const envFile = `.env.${env}`;
let resolvedEnvPath = path.resolve(process.cwd(), envFile);
if (!fs.existsSync(resolvedEnvPath)) {
  let tempDir = process.cwd();
  for (let i = 0; i < 3; i++) {
    tempDir = path.dirname(tempDir);
    const parentEnvPath = path.resolve(tempDir, envFile);
    if (fs.existsSync(parentEnvPath)) {
      resolvedEnvPath = parentEnvPath;
      break;
    }
  }
}
dotenv.config({ path: resolvedEnvPath });

import { LoggerFactory, LoggerService } from '@surgepay/common';
import { ConfigService, type LoggingConfig } from '@surgepay/config';

import { AppModule } from './app.module';
import { getGatewayConfig } from './config/gateway-config.schema';

async function bootstrap(): Promise<void> {
  // Use environment variables directly (with safe fallbacks) for the early bootstrapping logger phase
  const loggingConfig: LoggingConfig = {
    level: (process.env.LOG_LEVEL || 'info') as LoggingConfig['level'],
    pretty: process.env.LOG_PRETTY === 'true',
    serviceName: process.env.LOG_SERVICE_NAME || 'gateway',
  };

  const bootstrapLogger = LoggerFactory.createStandaloneLogger(loggingConfig);
  bootstrapLogger.info('Bootstrapping API Gateway service...');

  const app = await NestFactory.create(AppModule, {
    logger: bootstrapLogger,
    bufferLogs: true,
  });

  // Validate Gateway-specific configurations after environment is loaded
  getGatewayConfig();

  const configService = app.get(ConfigService);
  const logger = await app.resolve(LoggerService);

  // Apply the fully configured LoggerService to NestJS application
  app.useLogger(logger);

  // Enable Graceful Shutdown hooks
  app.enableShutdownHooks();

  // Configure CORS
  if (configService.security.corsEnabled) {
    app.enableCors({
      origin: '*',
      methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS',
      credentials: true,
    });
  }

  // Parse API prefix and version from the centralized configuration
  const apiPrefix = configService.http.apiPrefix;
  const parts = apiPrefix.split('/');
  const prefix = parts[0] || 'api';
  const defaultVersion = parts[1] ? parts[1].replace('v', '') : '1';

  // Apply routing prefix
  app.setGlobalPrefix(prefix);

  // Enable URI versioning dynamically based on config
  app.enableVersioning({
    type: VersioningType.URI,
    defaultVersion,
  });

  const port = configService.http.port || 3000;
  const host = configService.http.host || '0.0.0.0';

  await app.listen(port, host);
  logger.info(`API Gateway started successfully on http://${host}:${port}/${prefix}/v${defaultVersion}`);
}

bootstrap().catch((error) => {
  console.error('Fatal error during API Gateway bootstrapping:', error);
  process.exit(1);
});
// Trigger watch reload again


