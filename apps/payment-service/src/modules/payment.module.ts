import { Module } from '@nestjs/common';

import { LoggerModule } from '@surgepay/common';
import { CommonHttpModule } from '@surgepay/common-http';

import { PaymentController } from '../controllers/payment.controller';
import { PaymentRepository } from '../repositories/payment.repository';
import { PaymentService } from '../services/payment.service';

@Module({
  imports: [LoggerModule, CommonHttpModule],
  controllers: [PaymentController],
  providers: [PaymentService, PaymentRepository],
  exports: [PaymentService, PaymentRepository],
})
export class PaymentModule {}
