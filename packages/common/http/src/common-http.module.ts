import { Global, Module } from '@nestjs/common';

import { LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { ServiceRegistry } from './registry/service-registry';
import { ServiceClient } from './service-client';

@Global()
@Module({
  imports: [ConfigModule, LoggerModule],
  providers: [ServiceRegistry, ServiceClient],
  exports: [ServiceClient],
})
export class CommonHttpModule {}
