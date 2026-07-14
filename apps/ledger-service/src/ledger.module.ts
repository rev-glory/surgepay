import { Module } from '@nestjs/common';

import { KafkaEventProducer, LoggerModule, MetricsModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { LedgerCommandConsumer } from './handlers/ledger-command.consumer';
import { PrismaModule } from './prisma/prisma.module';
import { LedgerInboxRepository } from './repositories/inbox.repository';
import { LedgerRepository } from './repositories/ledger.repository';
import { OutboxRepository } from './repositories/outbox.repository';
import { LedgerService } from './services/ledger.service';
import { OutboxRelayWorker } from './services/outbox-relay.worker';

@Module({
  imports: [ConfigModule, LoggerModule, MetricsModule, PrismaModule],
  providers: [
    LedgerRepository,
    LedgerInboxRepository,
    OutboxRepository,
    LedgerService,
    OutboxRelayWorker,
    LedgerCommandConsumer,
    KafkaEventProducer,
  ],
  exports: [
    LedgerService,
    LedgerCommandConsumer,
  ],
})
export class LedgerModule {}
