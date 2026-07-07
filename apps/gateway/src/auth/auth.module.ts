import { Module } from '@nestjs/common';

import { LoggerModule } from '@surgepay/common';

import { MerchantAuthService } from './merchant-auth.service';
import { MerchantClientService } from './merchant-client.service';

@Module({
  imports: [LoggerModule],
  providers: [MerchantClientService, MerchantAuthService],
  exports: [MerchantAuthService, MerchantClientService],
})
export class AuthModule {}
