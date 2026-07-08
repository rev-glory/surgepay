import { Module } from '@nestjs/common';

import { LoggerModule } from '@surgepay/common';

import { OrderController } from '../controllers/order.controller';
import { OrderRepository } from '../repositories/order.repository';
import { OrderService } from '../services/order.service';

@Module({
  imports: [LoggerModule],
  controllers: [OrderController],
  providers: [OrderService, OrderRepository],
  exports: [OrderService, OrderRepository],
})
export class OrderModule {}
