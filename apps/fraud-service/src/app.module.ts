import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';

import { fraudConfig } from './config/fraud.config';
import { FraudModule } from './fraud/fraud.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [fraudConfig],
    }),
    FraudModule,
  ],
})
export class AppModule {}
