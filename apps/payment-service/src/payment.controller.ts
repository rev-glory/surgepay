import { Body, Controller, HttpCode, HttpStatus, Post, Req } from '@nestjs/common';
import { Request } from 'express';

import { LoggerService } from '@surgepay/common';

import { PaymentService } from './payment.service';

@Controller('payments')
export class PaymentController {
  constructor(
    private readonly paymentService: PaymentService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('PaymentController');
  }

  /**
   * Placeholder endpoint for handling payment proxying end-to-end.
   * Returns a static ACCEPTED status.
   */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED) // Returns 202 Accepted
  async createPayment(
    @Body() body: Record<string, unknown>,
    @Req() req: Request,
  ): Promise<{ status: string }> {
    this.logger.info('Received downstream payment request with headers', {
      requestId: req.headers['x-request-id'] as string | undefined,
      correlationId: req.headers['x-correlation-id'] as string | undefined,
      merchantId: req.headers['x-merchant-id'] as string | undefined,
    });
    return this.paymentService.createPlaceholderPayment(body);
  }
}
