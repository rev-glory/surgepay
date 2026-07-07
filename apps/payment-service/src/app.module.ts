import { Module } from '@nestjs/common';

import { HealthModule, LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { PaymentModule } from './payment.module';

@Module({
  imports: [
    ConfigModule,
    LoggerModule,
    HealthModule,
    PaymentModule,
  ],
})
export class AppModule {}
