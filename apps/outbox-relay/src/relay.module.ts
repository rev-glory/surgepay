import { Module } from '@nestjs/common';

import { LoggerModule, MetricsModule } from '@surgepay/common';
import { MessagingModule } from '@surgepay/common-messaging';
import { ConfigModule } from '@surgepay/config';

import { RelayMetrics } from './metrics.service';
import { Poller } from './poller';
import { PrismaService } from './prisma.service';
import { KafkaPublisher, OutboxPublisher } from './publisher';
import { RelayService } from './relay.service';
import { PollingScheduler } from './scheduler';

@Module({
  imports: [ConfigModule, LoggerModule, MessagingModule, MetricsModule],
  providers: [
    PrismaService,
    Poller,
    {
      provide: OutboxPublisher,
      useClass: KafkaPublisher,
    },
    RelayMetrics,
    RelayService,
    PollingScheduler,
  ],
  exports: [RelayService, PollingScheduler],
})
export class RelayModule {}
