import { Injectable } from '@nestjs/common';

import { CreatePaymentRequestDto, LoggerService } from '@surgepay/common';

import { PaymentRepository } from '../repositories/payment.repository';

@Injectable()
export class PaymentService {
  constructor(
    private readonly paymentRepository: PaymentRepository,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('PaymentService');
  }

  async createPlaceholderPayment(_body: CreatePaymentRequestDto): Promise<{ status: string }> {
    this.logger.info('Creating placeholder payment via service');
    return {
      status: 'ACCEPTED',
    };
  }

  async getPlaceholderPayment(id: string): Promise<{ id: string; status: string }> {
    this.logger.info('Retrieving placeholder payment via service', { id });
    return {
      id,
      status: 'PENDING',
    };
  }
}
