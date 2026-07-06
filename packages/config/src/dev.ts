/* eslint-disable no-console */
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

import 'reflect-metadata';

import { ConfigModule } from './config.module';
import { ConfigService } from './config.service';

@Module({
  imports: [ConfigModule],
})
class AppModule {}

async function bootstrap(): Promise<void> {
  try {
    // 1. Load the context to trigger the NestJS ConfigModule load and validation function.
    const app = await NestFactory.createApplicationContext(AppModule, { logger: false });

    // If we reached here, environment loading and validation succeeded.
    console.log('✓ Environment loaded');
    console.log('✓ Configuration validated');

    // 2. Retrieve ConfigService to assert typed initialization
    const configService = app.get(ConfigService);

    // Make sure we can access a value
    if (configService.database && configService.database.url) {
      console.log('✓ Configuration initialized');
    } else {
      throw new Error('Database URL configuration is missing or invalid');
    }

    console.log('Application starting...');

    await app.close();
  } catch (error: unknown) {
    console.error('❌ Application startup failed:', error);
    process.exit(1);
  }
}

void bootstrap();
