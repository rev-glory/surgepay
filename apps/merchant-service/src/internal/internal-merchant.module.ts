import { Module } from '@nestjs/common';

import { ApiKeysModule } from '../api-keys/api-keys.module';
import { InternalMerchantController } from './internal-merchant.controller';
import { InternalMerchantService } from './internal-merchant.service';

@Module({
  imports: [ApiKeysModule],
  controllers: [InternalMerchantController],
  providers: [InternalMerchantService],
})
export class InternalMerchantModule {}
