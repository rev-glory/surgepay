import { ConflictException, Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';

import { CreatePaymentRequestDto } from '../dto/create-payment-request.dto';
import { PaymentEntity } from '../entities/payment.entity';
import { PaymentRepository } from '../repositories/payment.repository';

@Injectable()
export class PaymentService {
  constructor(
    private readonly paymentRepository: PaymentRepository,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('PaymentService');
  }

  async createPayment(body: CreatePaymentRequestDto, merchantId: string): Promise<PaymentEntity> {
    const normalizedReference = body.reference.trim();

    // Business validation: Check duplicate payment reference for the same merchant
    const existing = await this.paymentRepository.findByReference(merchantId, normalizedReference);
    if (existing) {
      this.logger.warn('Duplicate payment reference detected', {
        merchantId,
        reference: normalizedReference,
      });
      throw new ConflictException(`Payment with reference '${normalizedReference}' already exists for this merchant.`);
    }

    // Create payment aggregate root
    const payment = PaymentEntity.create({
      merchantId,
      amount: body.amount,
      currency: body.currency,
      reference: normalizedReference,
    });

    // Persist via repository
    const persisted = await this.paymentRepository.create(payment);

    // Emit structured logs on success
    this.logger.info('Payment created successfully', {
      merchantId,
      paymentId: persisted.id,
      amount: persisted.amount,
      currency: persisted.currency,
      reference: persisted.reference,
      paymentStatus: persisted.status,
    });

    return persisted;
  }

  async getPayment(id: string): Promise<PaymentEntity | null> {
    this.logger.info('Retrieving payment by ID', { id });
    return this.paymentRepository.findById(id);
  }
}
