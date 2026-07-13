import { Module } from '@nestjs/common';

import { LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { OrderController } from '../controllers/order.controller';
import { OrderInboxRepository } from '../repositories/inbox.repository';
import { OrderRepository } from '../repositories/order.repository';
import { OrderService } from '../services/order.service';
import { OrderEventConsumer } from '../services/order-event.consumer';

@Module({
  imports: [LoggerModule, ConfigModule],
  controllers: [OrderController],
  providers: [
    OrderService,
    OrderRepository,
    OrderInboxRepository,
    OrderEventConsumer,
  ],
  exports: [
    OrderService,
    OrderRepository,
    OrderInboxRepository,
    OrderEventConsumer,
  ],
})
export class OrderModule {}
