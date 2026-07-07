import { Module } from '@nestjs/common';

import { LoggerModule } from '@surgepay/common';

import { IdempotencyInterceptor } from './idempotency.interceptor';
import { IdempotencyClientService } from './idempotency-client.service';

@Module({
  imports: [LoggerModule],
  providers: [IdempotencyClientService, IdempotencyInterceptor],
  exports: [IdempotencyClientService, IdempotencyInterceptor],
})
export class IdempotencyModule {}
