import { HttpStatus,Injectable, NestMiddleware } from '@nestjs/common';
import { NextFunction,Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';

import { createErrorResponse,LoggerService, RequestContextService } from '@surgepay/common';

import { RateLimitService } from './rate-limit.service';

@Injectable()
export class RateLimitMiddleware implements NestMiddleware {
  constructor(
    private readonly rateLimitService: RateLimitService,
    private readonly requestContext: RequestContextService,
    private readonly logger: LoggerService,
  ) {}

  async use(req: Request, res: Response, next: NextFunction): Promise<void> {
    const merchant = req.merchant;
    if (!merchant) {
      this.logger.warn('Rate Limit Middleware: Merchant context not found on request');
      return next();
    }

    const merchantId = merchant.merchantId;
    
    // Retrieve unique Request ID from response header (set by LoggingMiddleware) or context, with a fallback UUID.
    const rawResRequestId = res.getHeader('x-request-id') || res.getHeader('X-Request-Id');
    const requestId =
      (typeof rawResRequestId === 'string' ? rawResRequestId : undefined) ||
      this.requestContext.requestId ||
      `req_fallback_${uuidv4()}`;

    const result = await this.rateLimitService.checkLimit(merchantId, requestId);

    // Set standard rate limiting response headers
    res.setHeader('X-RateLimit-Limit', result.limit.toString());
    res.setHeader('X-RateLimit-Remaining', result.remaining.toString());

    const resetTimeSec = Math.ceil(result.resetTimeMs / 1000);
    res.setHeader('X-RateLimit-Reset', resetTimeSec.toString());

    if (!result.allowed) {
      // Calculate Retry-After: number of seconds until capacity is available
      const nowSec = Math.floor(Date.now() / 1000);
      const retryAfter = Math.max(1, resetTimeSec - nowSec);

      res.setHeader('Retry-After', retryAfter.toString());

      this.logger.warn(
        `Merchant rate limit exceeded for merchant: ${merchantId}. Retrying after ${retryAfter}s.`,
        {
          requestId,
          merchantId,
          limit: result.limit,
          resetTimeSec,
          retryAfter,
        },
      );

      res.status(HttpStatus.TOO_MANY_REQUESTS).json(
        createErrorResponse('RATE_LIMIT_EXCEEDED', 'Merchant rate limit exceeded.'),
      );
      return;
    }

    return next();
  }
}
