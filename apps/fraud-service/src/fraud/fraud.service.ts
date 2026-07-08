import { Inject, Injectable } from '@nestjs/common';

import { LoggerService, RequestContextService } from '@surgepay/common';

import { PrecheckRequestDto } from './dto/precheck-request.dto';
import { PrecheckResponseDto } from './dto/precheck-response.dto';
import { FraudRule } from './rules/fraud-rule.interface';

@Injectable()
export class FraudService {
  constructor(
    @Inject('FRAUD_RULES') private readonly rules: FraudRule[],
    private readonly requestContext: RequestContextService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('FraudService');
  }

  async runPrecheck(body: PrecheckRequestDto): Promise<PrecheckResponseDto> {
    const startTime = Date.now();
    const correlationId = this.requestContext.correlationId || 'N/A';

    // Sleep mapping for E2E timeout verification
    if (body.merchantId === '00000000-0000-4000-a000-000000000000') {
      this.logger.info('E2E Timeout trigger detected. Sleeping for 3000ms...');
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }

    this.logger.info('Initiating deterministic fraud screening checks', {
      correlationId,
      merchantId: body.merchantId,
      amount: body.amount,
      currency: body.currency,
    });

    let finalRiskScore = 10; // Base score when all rules pass
    let triggeredRule: string | undefined;
    let decision = 'APPROVED';
    let reason: string | undefined;

    for (const rule of this.rules) {
      const result = await rule.evaluate(body);

      if (!result.approved) {
        decision = 'REJECTED';
        triggeredRule = rule.name;
        finalRiskScore = result.riskScore;
        reason = result.reason;

        const durationMs = Date.now() - startTime;
        this.logger.warn('Payment screening request rejected', {
          correlationId,
          merchantId: body.merchantId,
          riskScore: finalRiskScore,
          decision,
          ruleTriggered: triggeredRule,
          reason,
          evaluationDurationMs: durationMs,
        });

        return {
          approved: false,
          riskScore: finalRiskScore,
          reason,
        };
      }

      // Accumulate risk scores for passing placeholder rules (Velocity etc.)
      finalRiskScore += result.riskScore;
    }

    const durationMs = Date.now() - startTime;
    this.logger.info('Payment screening request approved', {
      correlationId,
      merchantId: body.merchantId,
      riskScore: finalRiskScore,
      decision,
      evaluationDurationMs: durationMs,
    });

    return {
      approved: true,
      riskScore: finalRiskScore,
    };
  }
}
