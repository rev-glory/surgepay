import { Module } from '@nestjs/common';

import { LoggerModule } from '@surgepay/common';

import { PaymentController } from './payment.controller';
import { PaymentService } from './payment.service';

@Module({
  imports: [LoggerModule],
  controllers: [PaymentController],
  providers: [PaymentService],
})
export class PaymentModule {}
