import * as fs from 'fs';
import * as http from 'http';
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

import { LoggerFactory, LoggerService, MetricsService } from '@surgepay/common';
import type { LoggingConfig } from '@surgepay/config';

import { RelayModule } from './relay.module';

async function bootstrap(): Promise<void> {
  const loggingConfig: LoggingConfig = {
    level: (process.env.LOG_LEVEL || 'info') as LoggingConfig['level'],
    pretty: process.env.LOG_PRETTY === 'true',
    serviceName: process.env.LOG_SERVICE_NAME || 'outbox-relay',
  };

  const bootstrapLogger = LoggerFactory.createStandaloneLogger(loggingConfig);
  bootstrapLogger.info('Bootstrapping Outbox Relay Service...');

  const app = await NestFactory.createApplicationContext(RelayModule, {
    logger: bootstrapLogger,
    bufferLogs: true,
  });

  const logger = await app.resolve(LoggerService);
  app.useLogger(logger);

  app.enableShutdownHooks();

  const metricsPortStr = process.env.METRICS_PORT;
  const metricsPort = metricsPortStr ? parseInt(metricsPortStr, 10) : null;
  let metricsServer: http.Server | undefined;

  if (metricsPort) {
    const metricsService = app.get(MetricsService);
    metricsServer = http.createServer(async (req, res) => {
      if (req.url === '/metrics' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': metricsService.registry.contentType });
        res.end(await metricsService.registry.metrics());
      } else {
        res.writeHead(404);
        res.end();
      }
    });

    metricsServer.listen(metricsPort, '0.0.0.0', () => {
      logger.info(`Prometheus metrics endpoint listening on http://0.0.0.0:${metricsPort}/metrics`);
    });

    const originalClose = app.close.bind(app);
    app.close = async () => {
      logger.info('Closing Prometheus metrics server...');
      await new Promise<void>((resolve) => {
        metricsServer?.close(() => resolve());
      });
      await originalClose();
    };
  }

  logger.info('Outbox Relay Service bootstrapped successfully. Background worker running.');
}

bootstrap().catch((error) => {
  console.error('Fatal error during Outbox Relay Service bootstrapping:', error);
  process.exit(1);
});
