import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { ApiHeader, ApiOperation, ApiResponse, ApiSecurity, ApiTags } from '@nestjs/swagger';

import { CreatePaymentRequestDto, LoggerService, paymentRequestDuration,RequestContextService } from '@surgepay/common';

import { GatewayPaymentProxyService } from '../services/gateway-payment-proxy.service';

@ApiTags('Payments')
@ApiSecurity('X-API-Key')
@Controller('payments')
export class PaymentController {
  constructor(
    private readonly paymentProxy: GatewayPaymentProxyService,
    private readonly requestContext: RequestContextService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('PaymentController');
  }

  /**
   * Endpoint exposing public mutating payment requests.
   * Runs validation and idempotency checking via global middleware/interceptors,
   * then proxies transparently to the downstream Payment Service.
   *
   * @param body The payment payload.
   */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED) // Returns 202 Accepted
  @ApiOperation({
    summary: 'Process a payment request',
    description: 'Began the synchronous orchestration of a payment request. The gateway performs key checks, applies rate limits, and uses the Idempotency Service to prevent duplicate execution before proxying downstream.',
  })
  @ApiHeader({
    name: 'Idempotency-Key',
    description: 'Unique key (e.g. UUID) to prevent duplicate execution of mutating request operations',
    required: true,
  })
  @ApiResponse({
    status: 202,
    description: 'Payment request accepted and forwarded to the processing queue.',
    schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean', example: true },
        data: {
          type: 'object',
          properties: {
            paymentId: { type: 'string', example: 'pay_9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d' },
            status: { type: 'string', example: 'PENDING' },
            amount: { type: 'number', example: 99.99 },
            currency: { type: 'string', example: 'USD' },
            createdAt: { type: 'string', example: '2026-07-07T12:00:00.000Z' },
          },
        },
      },
    },
  })
  @ApiResponse({
    status: 400,
    description: 'Bad Request (validation error or missing Idempotency-Key).',
    schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean', example: false },
        error: {
          type: 'object',
          properties: {
            code: { type: 'string', example: 'VALIDATION_FAILED' },
            message: { type: 'string', example: 'Validation failed' },
            status: { type: 'number', example: 400 },
            timestamp: { type: 'string', example: '2026-07-07T12:00:00.000Z' },
            path: { type: 'string', example: '/api/v1/payments' },
            validationErrors: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  field: { type: 'string', example: 'amount' },
                  rejectedValue: { type: 'number', example: -10 },
                  rule: { type: 'string', example: 'isPositive' },
                  message: { type: 'string', example: 'amount must be a positive number' },
                },
              },
            },
          },
        },
      },
    },
  })
  @ApiResponse({
    status: 401,
    description: 'Unauthorized (invalid or missing API Key).',
    schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean', example: false },
        error: {
          type: 'object',
          properties: {
            code: { type: 'string', example: 'INVALID_API_KEY' },
            message: { type: 'string', example: 'Missing or invalid API key' },
            status: { type: 'number', example: 401 },
            timestamp: { type: 'string', example: '2026-07-07T12:00:00.000Z' },
            path: { type: 'string', example: '/api/v1/payments' },
          },
        },
      },
    },
  })
  @ApiResponse({
    status: 403,
    description: 'Forbidden (merchant account status disabled).',
    schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean', example: false },
        error: {
          type: 'object',
          properties: {
            code: { type: 'string', example: 'MERCHANT_DISABLED' },
            message: { type: 'string', example: 'Merchant status is inactive' },
            status: { type: 'number', example: 403 },
            timestamp: { type: 'string', example: '2026-07-07T12:00:00.000Z' },
            path: { type: 'string', example: '/api/v1/payments' },
          },
        },
      },
    },
  })
  @ApiResponse({
    status: 409,
    description: 'Conflict (idempotent request already in-flight).',
    schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean', example: false },
        error: {
          type: 'object',
          properties: {
            code: { type: 'string', example: 'IDEMPOTENCY_CONFLICT' },
            message: { type: 'string', example: 'An identical request with this Idempotency-Key is already in progress' },
            status: { type: 'number', example: 409 },
            timestamp: { type: 'string', example: '2026-07-07T12:00:00.000Z' },
            path: { type: 'string', example: '/api/v1/payments' },
          },
        },
      },
    },
  })
  @ApiResponse({
    status: 500,
    description: 'Internal server error.',
    schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean', example: false },
        error: {
          type: 'object',
          properties: {
            code: { type: 'string', example: 'INTERNAL_ERROR' },
            message: { type: 'string', example: 'An unexpected internal error occurred' },
            status: { type: 'number', example: 500 },
            timestamp: { type: 'string', example: '2026-07-07T12:00:00.000Z' },
            path: { type: 'string', example: '/api/v1/payments' },
          },
        },
      },
    },
  })
  async createPayment(@Body() body: CreatePaymentRequestDto): Promise<unknown> {
    const startTime = Date.now();
    const merchantId = this.requestContext.merchantId || 'N/A';
    try {
      const response = await this.paymentProxy.forwardPaymentRequest(body) as { paymentId?: string; status?: string };
      const durationMs = Date.now() - startTime;
      const paymentId = response?.paymentId;

      this.logger.info('Total request duration latency log', {
        requestId: this.requestContext.requestId || 'N/A',
        correlationId: this.requestContext.correlationId || 'N/A',
        merchantId,
        paymentId,
        stage: 'total-request-duration',
        durationMs,
      });

      paymentRequestDuration.record(durationMs, {
        merchantId,
        status: 'success',
      });

      return response;
    } catch (error) {
      const durationMs = Date.now() - startTime;

      this.logger.info('Total request duration latency log', {
        requestId: this.requestContext.requestId || 'N/A',
        correlationId: this.requestContext.correlationId || 'N/A',
        merchantId,
        stage: 'total-request-duration',
        durationMs,
      });

      paymentRequestDuration.record(durationMs, {
        merchantId,
        status: 'error',
      });

      throw error;
    }
  }
}
