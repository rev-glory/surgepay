import { Body, Controller, Get, HttpCode, HttpStatus, Param, Post, Req } from '@nestjs/common';
import { Request } from 'express';

import { CreatePaymentRequestDto, LoggerService } from '@surgepay/common';

import { PaymentService } from '../services/payment.service';

@Controller('payments')
export class PaymentController {
  constructor(
    private readonly paymentService: PaymentService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('PaymentController');
  }

  @Post()
  @HttpCode(HttpStatus.ACCEPTED) // Returns 202 Accepted
  async createPayment(
    @Body() body: CreatePaymentRequestDto,
    @Req() req: Request,
  ): Promise<{ status: string }> {
    this.logger.info('Received downstream payment request with headers', {
      requestId: req.headers['x-request-id'] as string | undefined,
      correlationId: req.headers['x-correlation-id'] as string | undefined,
      merchantId: req.headers['x-merchant-id'] as string | undefined,
    });
    return this.paymentService.createPlaceholderPayment(body);
  }

  @Get(':id')
  async getPayment(@Param('id') id: string): Promise<{ id: string; status: string }> {
    this.logger.info('Retrieving payment by ID', { id });
    return this.paymentService.getPlaceholderPayment(id);
  }
}
