import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { PrecheckRequestDto } from '../dto/precheck-request.dto';
import { FraudRule } from './fraud-rule.interface';

@Injectable()
export class UnsupportedCurrencyRule implements FraudRule {
  readonly name = 'UnsupportedCurrencyRule';

  constructor(private readonly configService: ConfigService) {}

  async evaluate(context: PrecheckRequestDto): Promise<{
    approved: boolean;
    riskScore: number;
    reason?: string;
  }> {
    const supported = this.configService.get<string[]>('fraud.supportedCurrencies') || [
      'INR',
      'USD',
      'EUR',
      'GBP',
    ];
    const currencyUpper = context.currency.trim().toUpperCase();

    if (!supported.includes(currencyUpper)) {
      return {
        approved: false,
        riskScore: 90,
        reason: 'UNSUPPORTED_CURRENCY',
      };
    }

    return {
      approved: true,
      riskScore: 0,
    };
  }
}
