import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';

import { getGatewayConfig } from '../config/gateway-config.schema';
import { RedisRateLimitRepository } from './redis-rate-limit.repository';

@Injectable()
export class RateLimitService {
  constructor(
    private readonly repository: RedisRateLimitRepository,
    private readonly logger: LoggerService,
  ) {}

  /**
   * Evaluates the rate limit quota for the merchant.
   *
   * @param merchantId Unique merchant ID
   * @param requestId Correlation / Request trace ID
   */
  async checkLimit(
    merchantId: string,
    requestId: string,
  ): Promise<{ allowed: boolean; limit: number; remaining: number; resetTimeMs: number }> {
    const config = getGatewayConfig();
    const limit = config.RATE_LIMIT_DEFAULT_LIMIT;
    const windowSeconds = config.RATE_LIMIT_WINDOW_SECONDS;

    try {
      const result = await this.repository.checkRateLimit(
        merchantId,
        limit,
        windowSeconds,
        requestId,
      );

      return {
        allowed: result.allowed,
        limit,
        remaining: result.remaining,
        resetTimeMs: result.resetTimeMs,
      };
    } catch (error: unknown) {
      // Robust error logging with correlation/request context
      this.logger.error(
        `Rate limiter Redis check failed for merchant ${merchantId}`,
        error instanceof Error ? error : new Error(String(error)),
        { merchantId, requestId },
      );

      // Fail-Open fallback to protect business continuity and avoid taking down the payment flow
      return {
        allowed: true,
        limit,
        remaining: 1,
        resetTimeMs: Date.now() + windowSeconds * 1000,
      };
    }
  }
}
