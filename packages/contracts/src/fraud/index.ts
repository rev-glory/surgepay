export interface FraudPrecheckRequest {
  merchantId: string;
  amount: number;
  currency: string;
}

export interface FraudPrecheckResponse {
  approved: boolean;
  riskScore: number;
  reason?: string;
}
