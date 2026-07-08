import { Body, Controller, HttpCode, HttpStatus, Post, Req } from '@nestjs/common';
import { Request } from 'express';

import { LoggerService } from '@surgepay/common';

import { ValidateOrderDto } from '../dto/validate-order.dto';
import { ValidateOrderResponseDto } from '../dto/validate-order-response.dto';
import { OrderService } from '../services/order.service';

@Controller('internal/orders')
export class OrderController {
  constructor(
    private readonly orderService: OrderService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('OrderController');
  }

  @Post('validate')
  @HttpCode(HttpStatus.OK) // Returns 200 OK on successful validation
  async validateOrder(
    @Body() body: ValidateOrderDto,
    @Req() req: Request,
  ): Promise<ValidateOrderResponseDto> {
    const correlationId = req.headers['x-correlation-id'] as string | undefined;
    const requestId = req.headers['x-request-id'] as string | undefined;

    this.logger.info('Handling order validation request', {
      requestId,
      correlationId,
      merchantId: body.merchantId,
      reference: body.reference,
      amount: body.amount,
      currency: body.currency,
    });

    const order = await this.orderService.validateOrder({
      merchantId: body.merchantId,
      reference: body.reference,
      amount: body.amount,
      currency: body.currency,
    });

    return {
      valid: true,
      orderId: order.id,
    };
  }
}
