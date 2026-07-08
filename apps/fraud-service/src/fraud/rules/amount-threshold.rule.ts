import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { PrecheckRequestDto } from '../dto/precheck-request.dto';
import { FraudRule } from './fraud-rule.interface';

@Injectable()
export class AmountThresholdRule implements FraudRule {
  readonly name = 'AmountThresholdRule';

  constructor(private readonly configService: ConfigService) {}

  async evaluate(context: PrecheckRequestDto): Promise<{
    approved: boolean;
    riskScore: number;
    reason?: string;
  }> {
    const maxThreshold = this.configService.get<number>('fraud.maxAmountThreshold') || 10000000;
    if (context.amount > maxThreshold) {
      return {
        approved: false,
        riskScore: 96,
        reason: 'AMOUNT_THRESHOLD_EXCEEDED',
      };
    }
    return {
      approved: true,
      riskScore: 0,
    };
  }
}
