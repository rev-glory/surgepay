import * as fs from 'fs';
import * as path from 'path';

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
import { RelayModule } from './relay.module';

async function bootstrap(): Promise<void> {
  const loggingConfig = {
    level: (process.env.LOG_LEVEL || 'info') as any,
    pretty: process.env.LOG_PRETTY === 'true',
    serviceName: process.env.LOG_SERVICE_NAME || 'outbox-relay',
  };

  const bootstrapLogger = LoggerFactory.createStandaloneLogger(loggingConfig);
  bootstrapLogger.info('Bootstrapping Outbox Relay Service...');

  // Create standard standalone application context for background tasks
  const app = await NestFactory.create(RelayModule, {
    logger: bootstrapLogger,
  });

  const logger = await app.resolve(LoggerService);
  app.useLogger(logger);

  // Enable Graceful Shutdown hooks
  app.enableShutdownHooks();

  const port = process.env.PORT || 3010;
  await app.listen(port, '0.0.0.0');
  logger.info(`Outbox Relay Service metrics server running on http://localhost:${port}`);
}

bootstrap().catch((error) => {
  console.error('Fatal error during Outbox Relay Service bootstrapping:', error);
  process.exit(1);
});
