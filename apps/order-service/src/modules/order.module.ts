import { Module } from '@nestjs/common';

import { KafkaEventProducer, LoggerModule, MetricsModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { OrderController } from '../controllers/order.controller';
import { OrderInboxRepository } from '../repositories/inbox.repository';
import { OrderRepository } from '../repositories/order.repository';
import { OrderService } from '../services/order.service';
import { OrderEventConsumer } from '../services/order-event.consumer';

@Module({
  imports: [LoggerModule, ConfigModule, MetricsModule],
  controllers: [OrderController],
  providers: [
    OrderService,
    OrderRepository,
    OrderInboxRepository,
    OrderEventConsumer,
    KafkaEventProducer,
  ],
  exports: [
    OrderService,
    OrderRepository,
    OrderInboxRepository,
    OrderEventConsumer,
    KafkaEventProducer,
  ],
})
export class OrderModule {}
