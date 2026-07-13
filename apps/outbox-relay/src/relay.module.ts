import { Module } from '@nestjs/common';

import { LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { OutboxPoller } from './poller';
import { PrismaModule } from './prisma/prisma.module';
import { ConsoleEventPublisher, EVENT_PUBLISHER } from './publisher';
import { OutboxRelayService } from './relay.service';
import { OutboxScheduler } from './scheduler';

@Module({
  imports: [
    ConfigModule,
    LoggerModule,
    PrismaModule,
  ],
  providers: [
    OutboxPoller,
    OutboxRelayService,
    OutboxScheduler,
    {
      provide: EVENT_PUBLISHER,
      useClass: ConsoleEventPublisher,
    },
  ],
})
export class RelayModule {}
