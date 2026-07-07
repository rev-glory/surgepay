import { Module } from '@nestjs/common';

import { HealthModule, LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { ApiKeysModule } from './api-keys/api-keys.module';
import { InternalMerchantModule } from './internal/internal-merchant.module';
import { PrismaModule } from './prisma/prisma.module';

@Module({
  imports: [
    ConfigModule,
    LoggerModule,
    HealthModule,
    PrismaModule,
    ApiKeysModule,
    InternalMerchantModule,
  ],
})
export class AppModule {}
