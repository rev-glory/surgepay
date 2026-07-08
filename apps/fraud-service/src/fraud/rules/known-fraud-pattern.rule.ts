import { Injectable } from '@nestjs/common';

import { PrecheckRequestDto } from '../dto/precheck-request.dto';
import { FraudRule } from './fraud-rule.interface';

@Injectable()
export class KnownFraudPatternRule implements FraudRule {
  readonly name = 'KnownFraudPatternRule';

  async evaluate(_context: PrecheckRequestDto): Promise<{
    approved: boolean;
    riskScore: number;
    reason?: string;
  }> {
    // Placeholder extension point for Day 5 fraud signatures.
    // Currently returns a pass with zero risk contribution.
    return {
      approved: true,
      riskScore: 0,
    };
  }
}
