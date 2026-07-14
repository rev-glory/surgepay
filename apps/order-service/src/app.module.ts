import { Module } from '@nestjs/common';

import { HealthModule, LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { OrderModule } from './modules/order.module';
import { PrismaModule } from './prisma/prisma.module';
import { SagaModule } from './saga/saga.module';

@Module({
  imports: [
    ConfigModule,
    LoggerModule,
    HealthModule,
    PrismaModule,
    OrderModule,
    SagaModule,
  ],
})
export class AppModule {}
