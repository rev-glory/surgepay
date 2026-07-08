import { Injectable } from '@nestjs/common';

import { ServiceClient } from '@surgepay/common-http';

@Injectable()
export class GatewayPaymentProxyService {
  constructor(private readonly serviceClient: ServiceClient) {}

  /**
   * Forwards a validated payment request downstream to the Payment Service.
   *
   * @param body The raw payment request body.
   */
  async forwardPaymentRequest(body: {
    amount: number;
    currency: string;
    reference?: string;
    orderId?: string;
  }): Promise<unknown> {
    const paymentServiceBody = {
      amount: body.amount,
      currency: body.currency,
      reference: body.reference || body.orderId,
    };
    return this.serviceClient.payment.post('/api/v1/payments', paymentServiceBody);
  }
}
