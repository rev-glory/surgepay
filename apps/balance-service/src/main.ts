import { NestFactory } from '@nestjs/core';
import { LoggerService } from '@surgepay/common';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  const logger = app.get(LoggerService);
  logger.setContext('BalanceService');
  logger.info('Balance Service initialized successfully');
}
bootstrap().catch((err) => {
  console.error('Fatal error during Balance Service startup:', err);
  process.exit(1);
});
