import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
  Req,
} from '@nestjs/common';
import { Request } from 'express';

import { LoggerService, PaymentNotFoundException } from '@surgepay/common';

import { CreatePaymentRequestDto } from '../dto/create-payment-request.dto';
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
  ): Promise<{ paymentId: string; status: string }> {
    const merchantId = req.headers['x-merchant-id'] as string | undefined;
    if (!merchantId) {
      this.logger.error('Missing required x-merchant-id header in payment request');
      throw new BadRequestException('Missing x-merchant-id header');
    }

    const correlationId = req.headers['x-correlation-id'] as string | undefined;
    const requestId = req.headers['x-request-id'] as string | undefined;

    this.logger.info('Received downstream payment creation request', {
      requestId,
      correlationId,
      merchantId,
      amount: body.amount,
      currency: body.currency,
      reference: body.reference,
    });

    const payment = await this.paymentService.createPayment(body, merchantId);

    return {
      paymentId: payment.id,
      status: payment.status,
    };
  }

  @Get(':id')
  async getPayment(
    @Param('id') id: string,
  ): Promise<{
    paymentId: string;
    merchantId: string;
    amount: number;
    currency: string;
    status: string;
    reference: string;
    createdAt: Date;
    updatedAt: Date;
  }> {
    const payment = await this.paymentService.getPayment(id);
    if (!payment) {
      throw new PaymentNotFoundException(id);
    }

    return {
      paymentId: payment.id,
      merchantId: payment.merchantId,
      amount: payment.amount,
      currency: payment.currency,
      status: payment.status,
      reference: payment.reference,
      createdAt: payment.createdAt,
      updatedAt: payment.updatedAt,
    };
  }
}
