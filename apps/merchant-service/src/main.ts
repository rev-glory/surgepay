import * as fs from 'fs';
import * as path from 'path';

import { VersioningType } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
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

import { AppValidationPipe, ExceptionLoggingFilter, LoggerFactory, LoggerService, LoggingInterceptor } from '@surgepay/common';
import { ConfigService, type LoggingConfig } from '@surgepay/config';

import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const loggingConfig: LoggingConfig = {
    level: (process.env.LOG_LEVEL || 'info') as LoggingConfig['level'],
    pretty: process.env.LOG_PRETTY === 'true',
    serviceName: process.env.LOG_SERVICE_NAME || 'merchant-service',
  };

  const bootstrapLogger = LoggerFactory.createStandaloneLogger(loggingConfig);
  bootstrapLogger.info('Bootstrapping Merchant Service...');

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

  // Register global filters, pipes, and interceptors in bootstrap (main.ts)
  app.useGlobalPipes(new AppValidationPipe());
  app.useGlobalFilters(new ExceptionLoggingFilter(logger));
  app.useGlobalInterceptors(new LoggingInterceptor(logger));

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

  // Use port 3001 for Merchant Service by default, or if PORT is set to 3000 (gateway's port)
  const port = process.env.PORT && process.env.PORT !== '3000'
    ? parseInt(process.env.PORT, 10)
    : 3001;
  const host = configService.http.host || '0.0.0.0';

  // Configure Swagger OpenAPI generation
  const swaggerConfig = new DocumentBuilder()
    .setTitle('SurgePay Merchant Service')
    .setDescription('Internal platform service managing merchant records, API credentials, configuration settings, and webhooks validation.')
    .setVersion('1.0.0')
    .setContact('SurgePay Support Placeholder', '', '')
    .setLicense('MIT', 'https://opensource.org/licenses/MIT')
    .addTag('Internal Merchants', 'Internal APIs for merchant credential validation and details retrieval')
    .addServer(`http://${host}:${port}`, 'Internal Merchant Service Server')
    .addApiKey(
      {
        type: 'apiKey',
        name: 'x-api-key',
        in: 'header',
        description: 'Internal API Key passed in the headers to check validation',
      },
      'X-API-Key',
    )
    .build();

  const swaggerDocument = SwaggerModule.createDocument(app, swaggerConfig);
  SwaggerModule.setup('swagger', app, swaggerDocument);

  await app.listen(port, host);
  logger.info(`Merchant Service started successfully on http://${host}:${port}/${prefix}/v${defaultVersion}`);
}

bootstrap().catch((error) => {
  console.error('Fatal error during Merchant Service bootstrapping:', error);
  process.exit(1);
});
// Trigger watch reload

