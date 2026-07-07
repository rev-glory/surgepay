import { Injectable } from '@nestjs/common';
import { Merchant, MerchantApiKey } from '@prisma/client';

import { LoggerService } from '@surgepay/common';

import { hashApiKey } from '../common/utils/hash-utils';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class ApiKeysService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly logger: LoggerService,
  ) {}

  /**
   * Looks up an API key by its SHA-256 hash.
   * Includes the associated merchant relation.
   *
   * @param rawKey The plain text API key.
   */
  async findActiveKeyWithMerchant(
    rawKey: string,
  ): Promise<(MerchantApiKey & { merchant: Merchant }) | null> {
    const apiKeyHash = hashApiKey(rawKey);

    const apiKeyRecord = await this.prisma.client.merchantApiKey.findUnique({
      where: { apiKeyHash },
      include: { merchant: true },
    });

    if (apiKeyRecord) {
      // Asynchronously update lastUsedAt without blocking the response flow
      this.updateLastUsedAtAsync(apiKeyRecord.id);
    }

    return apiKeyRecord;
  }

  /**
   * Updates lastUsedAt asynchronously and catches errors to prevent request pipeline failures.
   */
  private updateLastUsedAtAsync(keyId: string): void {
    this.prisma.client.merchantApiKey
      .update({
        where: { id: keyId },
        data: { lastUsedAt: new Date() },
      })
      .then(() => {
        this.logger.debug(`Successfully updated lastUsedAt for API key: ${keyId}`);
      })
      .catch((error) => {
        // Log the failure but do not propagate the exception (non-blocking)
        this.logger.error(`Failed to update lastUsedAt for API key ${keyId}: ${error.message}`, error);
      });
  }
}
