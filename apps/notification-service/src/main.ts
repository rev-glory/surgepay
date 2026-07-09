import { NestFactory } from '@nestjs/core';
import { LoggerService } from '@surgepay/common';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  const logger = app.get(LoggerService);
  app.useLogger(logger);
  app.enableShutdownHooks();

  const port = process.env.PORT || 3007;
  await app.listen(port, '0.0.0.0');
  logger.setContext('NotificationService');
  logger.info(`Notification Service running on http://localhost:${port}`);
}
bootstrap().catch((err) => {
  console.error('Fatal error during Notification Service startup:', err);
  process.exit(1);
});
