import { execSync } from 'child_process';
import { INestApplication, RequestMethod, ValidationPipe, VersioningType } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import Redis from 'ioredis';
import { AddressInfo } from 'net';
import { PrismaClient } from '@prisma/client';

import { AppModule as GatewayModule } from '../../apps/gateway/src/app.module';
import { AppModule as IdempotencyModule } from '../../apps/idempotency-service/src/app.module';
import { AppModule as MerchantModule } from '../../apps/merchant-service/src/app.module';
import { AppModule as PaymentModule } from '../../apps/payment-service/src/app.module';
import { AppModule as OrderModule } from '../../apps/order-service/src/app.module';

import {
  AppValidationPipe,
  ExceptionLoggingFilter,
  LoggerService,
  LoggingInterceptor,
} from '@surgepay/common';

import { PostgresTestContainer } from '../testcontainers/postgres.container';
import { RedisTestContainer } from '../testcontainers/redis.container';

let gatewayApp: INestApplication | null = null;
let merchantApp: INestApplication | null = null;
let idempotencyApp: INestApplication | null = null;
let paymentApp: INestApplication | null = null;
let orderApp: INestApplication | null = null;

let redisClient: Redis | null = null;
let prismaClient: PrismaClient | null = null;

let pgContainerInstance: PostgresTestContainer | null = null;
let redisContainerInstance: RedisTestContainer | null = null;

export function getRedisClient(): Redis {
  if (!redisClient) {
    throw new Error('Redis client not initialized. Call setupE2EEnvironment first.');
  }
  return redisClient;
}

export function getPrismaClient(): PrismaClient {
  if (!prismaClient) {
    throw new Error('Prisma client not initialized. Call setupE2EEnvironment first.');
  }
  return prismaClient;
}

export function getRedisContainerInstance(): RedisTestContainer | null {
  return redisContainerInstance;
}

export async function setupE2EEnvironment() {
  if (gatewayApp) {
    return {
      gatewayApp,
      merchantApp,
      idempotencyApp,
      paymentApp,
    };
  }

  // 1. Start Postgres Container if not already running in the process
  if (!pgContainerInstance) {
    pgContainerInstance = new PostgresTestContainer();
    const rawDbUrl = await pgContainerInstance.start();
    const dbUrlObj = new URL(rawDbUrl);
    dbUrlObj.searchParams.delete('schema');
    process.env.DATABASE_URL = dbUrlObj.toString();

    // Sync Prisma schema
    const merchantDatabaseUrl = new URL(process.env.DATABASE_URL);
    merchantDatabaseUrl.searchParams.set('schema', 'merchant');
    execSync('npx prisma db push --schema=apps/merchant-service/prisma/schema.prisma --skip-generate', {
      env: {
        ...process.env,
        DATABASE_URL: merchantDatabaseUrl.toString(),
      },
    });

    const paymentDatabaseUrl = new URL(process.env.DATABASE_URL);
    paymentDatabaseUrl.searchParams.set('schema', 'payment');
    execSync('npx prisma db push --schema=apps/payment-service/prisma/schema.prisma --skip-generate', {
      env: {
        ...process.env,
        DATABASE_URL: paymentDatabaseUrl.toString(),
      },
    });

    const orderDatabaseUrl = new URL(process.env.DATABASE_URL);
    orderDatabaseUrl.searchParams.set('schema', 'order');
    execSync('npx prisma db push --schema=apps/order-service/src/prisma/order.prisma --skip-generate', {
      env: {
        ...process.env,
        DATABASE_URL: orderDatabaseUrl.toString(),
      },
    });
  }

  // 2. Start Redis Container if not already running in the process
  if (!redisContainerInstance) {
    redisContainerInstance = new RedisTestContainer();
    process.env.REDIS_URL = await redisContainerInstance.start();
  }

  const databaseUrl = process.env.DATABASE_URL;
  const redisUrl = process.env.REDIS_URL;

  if (!databaseUrl || !redisUrl) {
    throw new Error('DATABASE_URL and REDIS_URL environment variables are not set.');
  }

  // 3. Initialize Clients
  const merchantDbUrlForClient = new URL(databaseUrl);
  merchantDbUrlForClient.searchParams.set('schema', 'merchant');
  prismaClient = new PrismaClient({
    datasources: {
      db: {
        url: merchantDbUrlForClient.toString(),
      },
    },
  });
  await prismaClient.$connect();

  redisClient = new Redis(redisUrl, {
    maxRetriesPerRequest: null,
  });

  // Set default service names for health check mapping
  process.env.NODE_ENV = 'test';
  process.env.REDIS_PASSWORD = '';

  // 4. Boot Merchant Service
  process.env.SERVICE_NAME = 'merchant-service';
  const merchantModuleFixture: TestingModule = await Test.createTestingModule({
    imports: [MerchantModule],
  }).compile();
  merchantApp = merchantModuleFixture.createNestApplication();
  merchantApp.setGlobalPrefix('api', {
    exclude: [
      { path: 'health', method: RequestMethod.ALL },
      { path: 'health/live', method: RequestMethod.ALL },
      { path: 'health/ready', method: RequestMethod.ALL },
    ],
  });
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

  // 5. Boot Idempotency Service
  process.env.SERVICE_NAME = 'idempotency-service';
  const idempotencyModuleFixture: TestingModule = await Test.createTestingModule({
    imports: [IdempotencyModule],
  }).compile();
  idempotencyApp = idempotencyModuleFixture.createNestApplication();
  idempotencyApp.setGlobalPrefix('api', {
    exclude: [
      { path: 'health', method: RequestMethod.ALL },
      { path: 'health/live', method: RequestMethod.ALL },
      { path: 'health/ready', method: RequestMethod.ALL },
    ],
  });
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

  // 6. Boot Payment Service
  process.env.SERVICE_NAME = 'payment-service';
  const paymentModuleFixture: TestingModule = await Test.createTestingModule({
    imports: [PaymentModule],
  }).compile();
  paymentApp = paymentModuleFixture.createNestApplication();
  paymentApp.setGlobalPrefix('api', {
    exclude: [
      { path: 'health', method: RequestMethod.ALL },
      { path: 'health/live', method: RequestMethod.ALL },
      { path: 'health/ready', method: RequestMethod.ALL },
    ],
  });
  paymentApp.enableVersioning({
    type: VersioningType.URI,
    defaultVersion: '1',
  });
  const paymentLogger = await paymentApp.resolve(LoggerService);
  paymentApp.useGlobalFilters(new ExceptionLoggingFilter(paymentLogger));
  paymentApp.useGlobalInterceptors(new LoggingInterceptor(paymentLogger));
  await paymentApp.listen(0);
  const paymentPort = (paymentApp.getHttpServer().address() as AddressInfo).port;
  process.env.PAYMENT_SERVICE_URL = `http://127.0.0.1:${paymentPort}`;

  // 7. Boot Order Service
  process.env.SERVICE_NAME = 'order-service';
  const orderModuleFixture: TestingModule = await Test.createTestingModule({
    imports: [OrderModule],
  }).compile();
  orderApp = orderModuleFixture.createNestApplication();
  orderApp.setGlobalPrefix('api', {
    exclude: [
      { path: 'health', method: RequestMethod.ALL },
      { path: 'health/live', method: RequestMethod.ALL },
      { path: 'health/ready', method: RequestMethod.ALL },
    ],
  });
  orderApp.enableVersioning({
    type: VersioningType.URI,
    defaultVersion: '1',
  });
  const orderLogger = await orderApp.resolve(LoggerService);
  orderApp.useGlobalFilters(new ExceptionLoggingFilter(orderLogger));
  orderApp.useGlobalInterceptors(new LoggingInterceptor(orderLogger));
  await orderApp.listen(0);
  const orderPort = (orderApp.getHttpServer().address() as AddressInfo).port;
  process.env.ORDER_SERVICE_URL = `http://127.0.0.1:${orderPort}`;

  // 8. Boot API Gateway
  process.env.SERVICE_NAME = 'gateway';
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

  // Clear SERVICE_NAME so individual services fall back to their ConfigService name for health checks
  delete process.env.SERVICE_NAME;

  return {
    gatewayApp,
    merchantApp,
    idempotencyApp,
    paymentApp,
    orderApp,
  };
}

export async function teardownE2EEnvironment() {
  if (gatewayApp) {
    await gatewayApp.close();
    gatewayApp = null;
  }
  if (merchantApp) {
    await merchantApp.close();
    merchantApp = null;
  }
  if (idempotencyApp) {
    await idempotencyApp.close();
    idempotencyApp = null;
  }
  if (paymentApp) {
    await paymentApp.close();
    paymentApp = null;
  }
  if (orderApp) {
    await orderApp.close();
    orderApp = null;
  }
  if (redisClient) {
    redisClient.disconnect();
    redisClient = null;
  }
  if (prismaClient) {
    await prismaClient.$disconnect();
    prismaClient = null;
  }
  await stopContainers();
}

export async function stopContainers() {
  if (redisContainerInstance) {
    await redisContainerInstance.stop();
    redisContainerInstance = null;
  }
  if (pgContainerInstance) {
    await pgContainerInstance.stop();
    pgContainerInstance = null;
  }
}
