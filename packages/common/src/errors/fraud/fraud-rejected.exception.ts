import { DomainException } from '../domain.exception';
import { DomainErrorCode } from '../error-codes';

export interface FraudRejectedMetadata extends Record<string, unknown> {
  merchantId: string;
  amount: number;
  currency: string;
  riskScore: number;
  reason?: string;
}

export class FraudRejectedException extends DomainException<FraudRejectedMetadata> {
  constructor(merchantId: string, amount: number, currency: string, riskScore: number, reason?: string, options?: { cause?: Error }) {
    super(
      'Payment rejected by fraud rules',
      DomainErrorCode.FRAUD_REJECTED,
      403,
      { merchantId, amount, currency, riskScore, reason },
      options,
    );
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
