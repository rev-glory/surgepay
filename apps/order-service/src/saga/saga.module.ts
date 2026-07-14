import { Module } from '@nestjs/common';

import { KafkaEventProducer, LoggerModule, MetricsModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { OrderInboxRepository } from '../repositories/inbox.repository';
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
  ],
  exports: [SagaService, SagaPaymentCompletedConsumer, SagaRepository],
})
export class SagaModule {}
