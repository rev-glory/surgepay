import { Module } from '@nestjs/common';

import { KafkaEventProducer, LoggerModule, MetricsModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { OrderInboxRepository } from '../repositories/inbox.repository';
import { CompensationCoordinator } from './compensation/compensation.coordinator';
import { CommandDispatcher } from './dispatchers/command.dispatcher';
import { SagaBalanceEventsConsumer } from './handlers/balance-events.handler';
import { SagaLedgerEventsConsumer } from './handlers/ledger-events.handler';
import { SagaOrderEventsConsumer } from './handlers/order-events.handler';
import { SagaPaymentCompletedConsumer } from './handlers/payment-completed.handler';
import { SagaRiskEventsConsumer } from './handlers/risk-events.handler';
import { OrderOutboxRelay } from './recovery/order-outbox.relay';
import { RetryEventsConsumer } from './recovery/retry-events.consumer';
import { SagaTimeoutScanner } from './recovery/saga-timeout.scanner';
import { SagaRecoveryService } from './recovery/saga-recovery.service';
import { OrderOutboxRepository } from './repositories/order-outbox.repository';
import { SagaRepository } from './repositories/saga.repository';
import { SagaService } from './saga.service';

@Module({
  imports: [ConfigModule, LoggerModule, MetricsModule],
  providers: [
    SagaService,
    CompensationCoordinator,
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
    SagaRecoveryService,
    KafkaEventProducer,
    SagaRepository,
    CommandDispatcher,
  ],
  exports: [
    SagaService,
    CompensationCoordinator,
    SagaPaymentCompletedConsumer,
    SagaOrderEventsConsumer,
    SagaLedgerEventsConsumer,
    SagaRiskEventsConsumer,
    SagaBalanceEventsConsumer,
    RetryEventsConsumer,
    SagaRepository,
    OrderOutboxRepository,
    CommandDispatcher,
    SagaRecoveryService,
  ],
})
export class SagaModule {}

