import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';

import { ExceptionLoggingFilter, LoggerService, RequestContextService } from '@surgepay/common';
import { ServiceClient } from '@surgepay/common-http';

import { AppModule as PaymentAppModule } from '../../apps/payment-service/src/app.module';
import { PrismaService } from '../../apps/payment-service/src/prisma/prisma.service';

export async function createPaymentTestApp(mockServiceClient: Record<string, unknown>): Promise<{
  app: INestApplication;
  prismaService: PrismaService;
  requestContext: RequestContextService;
}> {
  const originalUrl = process.env.DATABASE_URL;
  if (originalUrl) {
    const url = new URL(originalUrl);
    url.searchParams.delete('schema');
    process.env.DATABASE_URL = url.toString();
  }

  try {
    const moduleFixture = await Test.createTestingModule({
      imports: [PaymentAppModule],
    })
      .overrideProvider(ServiceClient)
      .useValue(mockServiceClient)
      .compile();

    const app = moduleFixture.createNestApplication();
    app.setGlobalPrefix('api'); // Match gateway routing prefix /api/payments/...

    const logger = await app.resolve(LoggerService);
    app.useGlobalFilters(new ExceptionLoggingFilter(logger));

    const prismaService = app.get(PrismaService);
    const requestContext = app.get(RequestContextService);

    await app.init();

    return { app, prismaService, requestContext };
  } finally {
    process.env.DATABASE_URL = originalUrl;
  }
}
