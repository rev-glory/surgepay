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
  async forwardPaymentRequest(body: unknown): Promise<unknown> {
    return this.serviceClient.payment.post('/api/v1/payments', body);
  }
}
