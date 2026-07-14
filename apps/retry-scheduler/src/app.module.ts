import { Module } from '@nestjs/common';
import { HealthModule, LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';
import { RetrySchedulerModule } from './retry-scheduler.module';
import { PrismaModule } from './prisma/prisma.module';

@Module({
  imports: [
    ConfigModule,
    LoggerModule,
    HealthModule,
    PrismaModule,
    RetrySchedulerModule,
  ],
})
export class AppModule {}
