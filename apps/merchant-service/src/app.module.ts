import { Module } from '@nestjs/common';

import { HealthModule, LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { PrismaModule } from './prisma/prisma.module';

@Module({
  imports: [
    ConfigModule,
    LoggerModule,
    HealthModule,
    PrismaModule,
  ],
})
export class AppModule {}
