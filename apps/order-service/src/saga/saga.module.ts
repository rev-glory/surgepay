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
import { OrderOutboxRepository } from './repositories/order-outbox.repository';
import { OrderOutboxRelay } from './recovery/order-outbox.relay';
import { SagaTimeoutScanner } from './recovery/saga-timeout.scanner';
import { RetryEventsConsumer } from './recovery/retry-events.consumer';

@Module({
  imports: [ConfigModule, LoggerModule, MetricsModule],
  providers: [
    SagaService,
    SagaPaymentCompletedConsumer,
    SagaOrderEventsConsumer,
    SagaLedgerEventsConsumer,
    SagaRiskEventsConsumer,
    SagaBalanceEventsConsumer,
    RetryEventsConsumer,
    OrderInboxRepository,
    OrderOutboxRepository,
    OrderOutboxRelay,
    SagaTimeoutScanner,
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
    RetryEventsConsumer,
    SagaRepository,
    OrderOutboxRepository,
    CommandDispatcher,
  ],
})
export class SagaModule {}

