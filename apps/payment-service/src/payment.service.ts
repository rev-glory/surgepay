import { Injectable } from '@nestjs/common';

@Injectable()
export class PaymentService {
  /**
   * Stub method for verifying Gateway-to-Payment-Service routing.
   * Returns a static response.
   */
  async createPlaceholderPayment(_body: Record<string, unknown>): Promise<{ status: string }> {
    return {
      status: 'ACCEPTED',
    };
  }
}
