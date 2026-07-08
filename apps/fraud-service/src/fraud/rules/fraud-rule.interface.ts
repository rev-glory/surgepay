import type { PrecheckRequestDto } from '../dto/precheck-request.dto';

export interface FraudRule {
  readonly name: string;
  evaluate(context: PrecheckRequestDto): Promise<{
    approved: boolean;
    riskScore: number;
    reason?: string;
  }>;
}
