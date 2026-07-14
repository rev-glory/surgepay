import { Module } from '@nestjs/common';

import { KafkaEventProducer, LoggerModule, MetricsModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { OrderInboxRepository } from '../repositories/inbox.repository';
import { CommandDispatcher } from './dispatchers/command.dispatcher';
import { SagaBalanceEventsConsumer } from './handlers/balance-events.handler';
import { SagaLedgerEventsConsumer } from './handlers/ledger-events.handler';
import { SagaOrderEventsConsumer } from './handlers/order-events.handler';
import { SagaPaymentCompletedConsumer } from './handlers/payment-completed.handler';
import { SagaRiskEventsConsumer } from './handlers/risk-events.handler';
import { SagaRepository } from './repositories/saga.repository';
import { SagaService } from './saga.service';

@Module({
  imports: [ConfigModule, LoggerModule, MetricsModule],
  providers: [
    SagaService,
    SagaPaymentCompletedConsumer,
    SagaOrderEventsConsumer,
    SagaLedgerEventsConsumer,
    SagaRiskEventsConsumer,
    SagaBalanceEventsConsumer,
    OrderInboxRepository,
    KafkaEventProducer,
    SagaRepository,
    CommandDispatcher,
  ],
  exports: [
    SagaService,
    SagaPaymentCompletedConsumer,
    SagaOrderEventsConsumer,
    SagaLedgerEventsConsumer,
    SagaRiskEventsConsumer,
    SagaBalanceEventsConsumer,
    SagaRepository,
    CommandDispatcher,
  ],
})
export class SagaModule {}

