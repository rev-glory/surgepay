import { Module } from '@nestjs/common';
import { LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { PrismaService } from './prisma.service';
import { Poller } from './poller';
import { ConsolePublisher, OutboxPublisher } from './publisher';
import { RelayMetrics } from './metrics.service';
import { RelayService } from './relay.service';
import { PollingScheduler } from './scheduler';

@Module({
  imports: [ConfigModule, LoggerModule],
  providers: [
    PrismaService,
    Poller,
    {
      provide: OutboxPublisher,
      useClass: ConsolePublisher,
    },
    RelayMetrics,
    RelayService,
    PollingScheduler,
  ],
  exports: [RelayService, PollingScheduler],
})
export class RelayModule {}
