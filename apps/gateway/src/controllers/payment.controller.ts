import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';

import { CreatePaymentRequestDto, RequestContextService } from '@surgepay/common';

import { GatewayPaymentProxyService } from '../services/gateway-payment-proxy.service';

@Controller('payments')
export class PaymentController {
  constructor(
    private readonly paymentProxy: GatewayPaymentProxyService,
    private readonly requestContext: RequestContextService,
  ) {}

  /**
   * Endpoint exposing public mutating payment requests.
   * Runs validation and idempotency checking via global middleware/interceptors,
   * then proxies transparently to the downstream Payment Service.
   *
   * @param body The payment payload.
   */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED) // Returns 202 Accepted
  async createPayment(@Body() body: CreatePaymentRequestDto): Promise<unknown> {
    return this.paymentProxy.forwardPaymentRequest(body);
  }
}
