import { Injectable } from '@nestjs/common';

import { CreatePaymentRequestDto } from '@surgepay/common';

@Injectable()
export class PaymentService {
  /**
   * Stub method for verifying Gateway-to-Payment-Service routing.
   * Returns a static response.
   */
  async createPlaceholderPayment(_body: CreatePaymentRequestDto): Promise<{ status: string }> {
    return {
      status: 'ACCEPTED',
    };
  }
}
