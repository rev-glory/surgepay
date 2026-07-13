import type { INestApplication} from '@nestjs/common';
import { ValidationPipe, VersioningType } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { Test } from '@nestjs/testing';
import { PrismaClient } from '@prisma/client';
import Redis from 'ioredis';
import type { AddressInfo } from 'net';

import {
  AppValidationPipe,
  ExceptionLoggingFilter,
  LoggerService,
  LoggingInterceptor,
} from '@surgepay/common';

import { AppModule as GatewayModule } from '../../../apps/gateway/src/app.module';
import { AppModule as IdempotencyModule } from '../../../apps/idempotency-service/src/app.module';
import { AppModule as MerchantModule } from '../../../apps/merchant-service/src/app.module';

let gatewayApp: INestApplication | null = null;
let merchantApp: INestApplication | null = null;
let idempotencyApp: INestApplication | null = null;

let redisClient: Redis | null = null;
let prismaClient: PrismaClient | null = null;

export function getRedisClient(): Redis {
  if (!redisClient) {
    throw new Error('Redis client not initialized. Call setupIntegrationEnvironment first.');
  }
  return redisClient;
}

export function getPrismaClient(): PrismaClient {
  if (!prismaClient) {
    throw new Error('Prisma client not initialized. Call setupIntegrationEnvironment first.');
  }
  return prismaClient;
}

export async function setupIntegrationEnvironment() {
  if (gatewayApp) {
    return {
      gatewayApp,
      merchantApp,
      idempotencyApp,
    };
  }

  const databaseUrl = process.env.DATABASE_URL;
  const redisUrl = process.env.REDIS_URL;

  if (!databaseUrl || !redisUrl) {
    throw new Error('DATABASE_URL and REDIS_URL env variables must be set.');
  }

  // 1. Initialize Clients
  prismaClient = new PrismaClient({
    datasources: {
      db: {
        url: databaseUrl,
      },
    },
  });
  await prismaClient.$connect();

  redisClient = new Redis(redisUrl);

  // 2. Boot Merchant Service
  const merchantModuleFixture: TestingModule = await Test.createTestingModule({
    imports: [MerchantModule],
  }).compile();
  merchantApp = merchantModuleFixture.createNestApplication();
  merchantApp.setGlobalPrefix('api');
  merchantApp.enableVersioning({
    type: VersioningType.URI,
    defaultVersion: '1',
  });
  merchantApp.useGlobalPipes(new AppValidationPipe());
  const merchantLogger = await merchantApp.resolve(LoggerService);
  merchantApp.useGlobalFilters(new ExceptionLoggingFilter(merchantLogger));
  merchantApp.useGlobalInterceptors(new LoggingInterceptor(merchantLogger));
  await merchantApp.listen(0);
  const merchantPort = (merchantApp.getHttpServer().address() as AddressInfo).port;
  process.env.MERCHANT_SERVICE_URL = `http://127.0.0.1:${merchantPort}`;

  // 3. Boot Idempotency Service
  const idempotencyModuleFixture: TestingModule = await Test.createTestingModule({
    imports: [IdempotencyModule],
  }).compile();
  idempotencyApp = idempotencyModuleFixture.createNestApplication();
  idempotencyApp.setGlobalPrefix('api');
  idempotencyApp.enableVersioning({
    type: VersioningType.URI,
    defaultVersion: '1',
  });
  idempotencyApp.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );
  const idempotencyLogger = await idempotencyApp.resolve(LoggerService);
  idempotencyApp.useGlobalFilters(new ExceptionLoggingFilter(idempotencyLogger));
  idempotencyApp.useGlobalInterceptors(new LoggingInterceptor(idempotencyLogger));
  await idempotencyApp.listen(0);
  const idempotencyPort = (idempotencyApp.getHttpServer().address() as AddressInfo).port;
  process.env.IDEMPOTENCY_SERVICE_URL = `http://127.0.0.1:${idempotencyPort}`;

  // 4. Boot API Gateway
  const gatewayModuleFixture: TestingModule = await Test.createTestingModule({
    imports: [GatewayModule],
  }).compile();
  gatewayApp = gatewayModuleFixture.createNestApplication();
  gatewayApp.setGlobalPrefix('api');
  gatewayApp.enableVersioning({
    type: VersioningType.URI,
    defaultVersion: '1',
  });
  await gatewayApp.listen(0);

  return {
    gatewayApp,
    merchantApp,
    idempotencyApp,
  };
}

export async function teardownIntegrationEnvironment() {
  try {
    if (gatewayApp) {
      await gatewayApp.close();
    }
  } catch (err) {
    console.error('Error closing gatewayApp during teardown:', err);
  } finally {
    gatewayApp = null;
  }

  try {
    if (merchantApp) {
      await merchantApp.close();
    }
  } catch (err) {
    console.error('Error closing merchantApp during teardown:', err);
  } finally {
    merchantApp = null;
  }

  try {
    if (idempotencyApp) {
      await idempotencyApp.close();
    }
  } catch (err) {
    console.error('Error closing idempotencyApp during teardown:', err);
  } finally {
    idempotencyApp = null;
  }

  try {
    if (redisClient) {
      if (redisClient.status !== 'end') {
        await redisClient.quit();
      }
    }
  } catch (err) {
    console.error('Error quitting redisClient during teardown:', err);
  } finally {
    redisClient = null;
  }

  try {
    if (prismaClient) {
      await prismaClient.$disconnect();
    }
  } catch (err) {
    console.error('Error disconnecting prismaClient during teardown:', err);
  } finally {
    prismaClient = null;
  }
}
