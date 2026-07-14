import { Module } from '@nestjs/common';

import { KafkaEventProducer, LoggerModule, MetricsModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { BalanceCommandConsumer } from './handlers/balance-command.consumer';
import { PrismaModule } from './prisma/prisma.module';
import { BalanceRepository } from './repositories/balance.repository';
import { BalanceInboxRepository } from './repositories/inbox.repository';
import { OutboxRepository } from './repositories/outbox.repository';
import { BalanceService } from './services/balance.service';
import { OutboxRelayWorker } from './services/outbox-relay.worker';

@Module({
  imports: [ConfigModule, LoggerModule, MetricsModule, PrismaModule],
  providers: [
    BalanceRepository,
    BalanceInboxRepository,
    OutboxRepository,
    BalanceService,
    OutboxRelayWorker,
    BalanceCommandConsumer,
    KafkaEventProducer,
  ],
  exports: [
    BalanceService,
    BalanceCommandConsumer,
  ],
})
export class BalanceModule {}
