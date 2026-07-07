import { Controller, Get, Headers } from '@nestjs/common';
import { ApiHeader, ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';

import { ValidateMerchantResponse } from '@surgepay/contracts';

import { InternalMerchantService } from './internal-merchant.service';

@ApiTags('Internal Merchants')
@Controller('internal/merchants')
export class InternalMerchantController {
  constructor(private readonly internalMerchantService: InternalMerchantService) {}

  /**
   * Internal endpoint used by the API Gateway to validate merchant credentials.
   *
   * @param apiKey The API key passed in the headers.
   */
  @Get('validate')
  @ApiOperation({
    summary: 'Validate merchant by API key',
    description: 'Internal API key validation endpoint used downstream by the Gateway. Verifies whether the key is active and links to an active merchant.',
  })
  @ApiHeader({
    name: 'x-api-key',
    description: 'The raw merchant API key to validate',
    required: true,
  })
  @ApiResponse({
    status: 200,
    description: 'Merchant API key is valid. Returns merchant permissions and status.',
    schema: {
      type: 'object',
      properties: {
        merchantId: { type: 'string', example: '9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d' },
        merchantName: { type: 'string', example: 'Acme Merchant' },
        status: { type: 'string', example: 'ACTIVE', enum: ['ACTIVE', 'INACTIVE', 'SUSPENDED'] },
        permissions: { type: 'array', items: { type: 'string' }, example: ['payment:create', 'payment:refund'] },
        webhookEnabled: { type: 'boolean', example: true },
      },
    },
  })
  @ApiResponse({
    status: 401,
    description: 'Unauthorized (invalid, revoked, or missing API Key).',
    schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean', example: false },
        error: {
          type: 'object',
          properties: {
            code: { type: 'string', example: 'INVALID_API_KEY' },
            message: { type: 'string', example: 'Invalid API key' },
            status: { type: 'number', example: 401 },
            timestamp: { type: 'string', example: '2026-07-07T12:00:00.000Z' },
            path: { type: 'string', example: '/api/v1/internal/merchants/validate' },
          },
        },
      },
    },
  })
  @ApiResponse({
    status: 403,
    description: 'Forbidden (merchant account status is inactive or suspended).',
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
            path: { type: 'string', example: '/api/v1/internal/merchants/validate' },
          },
        },
      },
    },
  })
  async validate(
    @Headers('x-api-key') apiKey: string,
  ): Promise<ValidateMerchantResponse> {
    return this.internalMerchantService.validateMerchantApiKey(apiKey);
  }
}
