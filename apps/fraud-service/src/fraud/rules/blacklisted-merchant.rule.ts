import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { PrecheckRequestDto } from '../dto/precheck-request.dto';
import { FraudRule } from './fraud-rule.interface';

@Injectable()
export class BlacklistedMerchantRule implements FraudRule {
  readonly name = 'BlacklistedMerchantRule';

  constructor(private readonly configService: ConfigService) {}

  async evaluate(context: PrecheckRequestDto): Promise<{
    approved: boolean;
    riskScore: number;
    reason?: string;
  }> {
    const blacklist = this.configService.get<string[]>('fraud.blacklistedMerchants') || [];

    if (blacklist.includes(context.merchantId)) {
      return {
        approved: false,
        riskScore: 99,
        reason: 'MERCHANT_BLACKLISTED',
      };
    }

    return {
      approved: true,
      riskScore: 0,
    };
  }
}
