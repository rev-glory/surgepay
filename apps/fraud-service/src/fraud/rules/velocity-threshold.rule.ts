import { Injectable } from '@nestjs/common';

import { PrecheckRequestDto } from '../dto/precheck-request.dto';
import { FraudRule } from './fraud-rule.interface';

@Injectable()
export class VelocityThresholdRule implements FraudRule {
  readonly name = 'VelocityThresholdRule';

  async evaluate(_context: PrecheckRequestDto): Promise<{
    approved: boolean;
    riskScore: number;
    reason?: string;
  }> {
    // Placeholder extension point for Day 5 velocity calculations.
    // Currently returns a pass with nominal risk contribution.
    return {
      approved: true,
      riskScore: 2,
    };
  }
}
