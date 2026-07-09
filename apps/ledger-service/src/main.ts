import { NestFactory } from '@nestjs/core';
import { LoggerService } from '@surgepay/common';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  const logger = app.get(LoggerService);
  logger.setContext('LedgerService');
  logger.info('Ledger Service initialized successfully');
}
bootstrap().catch((err) => {
  console.error('Fatal error during Ledger Service startup:', err);
  process.exit(1);
});
