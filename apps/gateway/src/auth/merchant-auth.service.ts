import { Injectable } from '@nestjs/common';

import { MerchantContext } from './interfaces/merchant-context.interface';
import { MerchantClientService } from './merchant-client.service';

@Injectable()
export class MerchantAuthService {
  constructor(private readonly merchantClient: MerchantClientService) {}

  /**
   * Validates the credentials against the Merchant Service.
   *
   * @param apiKey The API key passed in headers.
   */
  async authenticate(apiKey: string): Promise<MerchantContext> {
    const validationResult = await this.merchantClient.validateApiKey(apiKey);

    return {
      merchantId: validationResult.merchantId,
      merchantName: validationResult.merchantName,
      permissions: validationResult.permissions,
    };
  }
}
