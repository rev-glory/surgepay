import { VersioningType } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

import { LoggerFactory, LoggerService } from '@surgepay/common';
import { ConfigService, type LoggingConfig } from '@surgepay/config';

import { AppModule } from './app.module';

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
