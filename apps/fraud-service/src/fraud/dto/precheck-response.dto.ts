import type { FraudPrecheckResponse } from '@surgepay/contracts';

export class PrecheckResponseDto implements FraudPrecheckResponse {
  approved!: boolean;
  riskScore!: number;
  reason?: string;
}
