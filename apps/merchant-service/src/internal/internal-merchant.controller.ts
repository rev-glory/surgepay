import { Controller, Get, Headers } from '@nestjs/common';

import { ValidateMerchantResponse } from '@surgepay/contracts';

import { InternalMerchantService } from './internal-merchant.service';

@Controller('internal/merchants')
export class InternalMerchantController {
  constructor(private readonly internalMerchantService: InternalMerchantService) {}

  /**
   * Internal endpoint used by the API Gateway to validate merchant credentials.
   *
   * @param apiKey The API key passed in the headers.
   */
  @Get('validate')
  async validate(
    @Headers('x-api-key') apiKey: string,
  ): Promise<ValidateMerchantResponse> {
    return this.internalMerchantService.validateMerchantApiKey(apiKey);
  }
}
