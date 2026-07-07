import { Module } from '@nestjs/common';

import { LoggerModule } from '@surgepay/common';

import { PrismaModule } from '../prisma/prisma.module';
import { ApiKeysService } from './api-keys.service';

@Module({
  imports: [PrismaModule, LoggerModule],
  providers: [ApiKeysService],
  exports: [ApiKeysService],
})
export class ApiKeysModule {}
