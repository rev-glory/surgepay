import { Module } from '@nestjs/common';
import { KafkaEventProducer, LoggerModule, MetricsModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';
import { PrismaService } from './prisma/prisma.service';
import { RetryInboxRepository } from './repositories/inbox.repository';
import { RetryRepository } from './repositories/retry.repository';
import { RetrySchedulerService } from './services/retry-scheduler.service';
import { RetryPollerService } from './services/poller.service';
import { RetryCommandConsumer } from './handlers/retry-command.consumer';

@Module({
  imports: [ConfigModule, LoggerModule, MetricsModule],
  providers: [
    PrismaService,
    RetryInboxRepository,
    RetryRepository,
    RetrySchedulerService,
    RetryPollerService,
    RetryCommandConsumer,
    KafkaEventProducer,
  ],
  exports: [
    PrismaService,
    RetryInboxRepository,
    RetryRepository,
    RetrySchedulerService,
    RetryPollerService,
    RetryCommandConsumer,
  ],
})
export class RetrySchedulerModule {}
