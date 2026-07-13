import { Module } from '@nestjs/common';

import { KafkaEventProducer, LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { OutboxPoller } from './poller';
import { PrismaModule } from './prisma/prisma.module';
import { EVENT_PUBLISHER, KafkaOutboxPublisher } from './publisher';
import { OutboxRepository } from './repositories/outbox.repository';
import { OutboxRelayService } from './relay.service';
import { OutboxScheduler } from './scheduler';

@Module({
  imports: [
    ConfigModule,
    LoggerModule,
    PrismaModule,
  ],
  providers: [
    OutboxRepository,
    OutboxPoller,
    OutboxRelayService,
    OutboxScheduler,
    KafkaEventProducer,
    {
      provide: EVENT_PUBLISHER,
      useClass: KafkaOutboxPublisher,
    },
  ],
})
export class RelayModule {}
