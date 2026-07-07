import { Global, Module } from '@nestjs/common';

import { HealthDatabaseClient } from '@surgepay/common';

import { PrismaService } from './prisma.service';

@Global()
@Module({
  providers: [
    PrismaService,
    {
      provide: HealthDatabaseClient,
      useExisting: PrismaService,
    },
  ],
  exports: [PrismaService, HealthDatabaseClient],
})
export class PrismaModule {}
