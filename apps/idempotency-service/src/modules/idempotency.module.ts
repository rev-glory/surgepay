import { Module } from '@nestjs/common';
import { ConfigModule as NestConfigModule } from '@nestjs/config';

import { LoggerModule } from '@surgepay/common';

import idempotencyConfig from '../config/idempotency.config';
import { IdempotencyController } from '../controllers/idempotency.controller';
import { IdempotencyService } from '../services/idempotency.service';
import { RequestHashService } from '../services/request-hash.service';

@Module({
  imports: [NestConfigModule.forFeature(idempotencyConfig), LoggerModule],
  controllers: [IdempotencyController],
  providers: [IdempotencyService, RequestHashService],
  exports: [IdempotencyService, RequestHashService],
})
export class IdempotencyModule {}
