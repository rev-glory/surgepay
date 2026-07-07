import { ForbiddenException, Injectable, UnauthorizedException } from '@nestjs/common';

import { ValidateMerchantResponse } from '@surgepay/contracts';

import { ApiKeysService } from '../api-keys/api-keys.service';

@Injectable()
export class InternalMerchantService {
  constructor(private readonly apiKeysService: ApiKeysService) {}

  /**
   * Validates an API key and returns the associated merchant information.
   *
   * @param apiKey The plain text API key.
   * @throws UnauthorizedException if the API key is missing, not found, inactive, or revoked.
   * @throws ForbiddenException if the associated merchant status is not ACTIVE.
   */
  async validateMerchantApiKey(apiKey: string): Promise<ValidateMerchantResponse> {
    if (!apiKey) {
      throw new UnauthorizedException('Missing API key');
    }

    const keyRecord = await this.apiKeysService.findActiveKeyWithMerchant(apiKey);

    if (!keyRecord || !keyRecord.active || keyRecord.revokedAt !== null) {
      throw new UnauthorizedException('Invalid API key');
    }

    const merchant = keyRecord.merchant;

    if (merchant.status !== 'ACTIVE') {
      throw new ForbiddenException('Merchant status is inactive');
    }

    // Extract dynamic configurations from metadata JSON
    const metadata = (merchant.metadata || {}) as Record<string, unknown>;
    const permissions = Array.isArray(metadata.permissions)
      ? (metadata.permissions as string[])
      : [];
    const webhookEnabled =
      typeof metadata.webhookEnabled === 'boolean' ? metadata.webhookEnabled : true;

    return {
      merchantId: merchant.merchantId,
      merchantName: merchant.name,
      status: merchant.status as 'ACTIVE' | 'INACTIVE' | 'SUSPENDED',
      permissions,
      webhookEnabled,
    };
  }
}
