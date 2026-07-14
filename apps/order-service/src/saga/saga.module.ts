import { Module } from '@nestjs/common';

import { KafkaEventProducer, LoggerModule, MetricsModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { OrderInboxRepository } from '../repositories/inbox.repository';
import { CommandDispatcher } from './dispatchers/command.dispatcher';
import { SagaPaymentCompletedConsumer } from './handlers/payment-completed.handler';
import { SagaRepository } from './repositories/saga.repository';
import { SagaService } from './saga.service';

@Module({
  imports: [ConfigModule, LoggerModule, MetricsModule],
  providers: [
    SagaService,
    SagaPaymentCompletedConsumer,
    OrderInboxRepository,
    KafkaEventProducer,
    SagaRepository,
    CommandDispatcher,
  ],
  exports: [SagaService, SagaPaymentCompletedConsumer, SagaRepository, CommandDispatcher],
})
export class SagaModule {}
