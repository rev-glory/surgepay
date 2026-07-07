import { HttpException, HttpStatus, Injectable, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';

import { LoggerService, RequestContextService } from '@surgepay/common';

import { MerchantAuthService } from './merchant-auth.service';

@Injectable()
export class AuthMiddleware implements NestMiddleware {
  constructor(
    private readonly authService: MerchantAuthService,
    private readonly requestContext: RequestContextService,
    private readonly logger: LoggerService,
  ) {}

  async use(req: Request, res: Response, next: NextFunction): Promise<void> {
    const path = req.path;
    const method = req.method;
    const requestId = this.requestContext.requestId || 'N/A';

    // 1. Bypass authentication for health probe endpoints (use originalUrl to avoid router rewrites)
    const originalUrl = (req.originalUrl || '').split('?')[0] || '';
    if (
      originalUrl.endsWith('/health') ||
      originalUrl.endsWith('/health/live') ||
      originalUrl.endsWith('/health/ready') ||
      originalUrl.includes('/health/')
    ) {
      return next();
    }

    // 2. Extract API key from headers (case-insensitive lookup)
    const apiKeyHeader = req.headers['x-api-key'] || req.headers['X-API-Key'];
    const apiKey = typeof apiKeyHeader === 'string' ? apiKeyHeader : undefined;

    if (!apiKey) {
      this.logger.warn(`Authentication failed: Missing API key. Path: ${path}`, {
        requestId,
        method,
        path,
        outcome: 'FAILED_MISSING_KEY',
      });
      throw new HttpException('Missing API key', HttpStatus.UNAUTHORIZED);
    }

    const startTime = Date.now();

    try {
      // 3. Delegate validation to the Merchant Service via the Auth Service
      const merchantContext = await this.authService.authenticate(apiKey);
      const latency = Date.now() - startTime;

      // 4. Attach merchant context to the Request object
      req.merchant = merchantContext;

      // Update RequestContext store and headers with the authenticated merchant ID
      req.headers['x-merchant-id'] = merchantContext.merchantId;
      const store = this.requestContext.getStore();
      if (store) {
        store.merchantId = merchantContext.merchantId;
      }

      // 5. Produce structured outcome logs without exposing the key
      this.logger.info(`Authentication successful for merchant: ${merchantContext.merchantId}`, {
        requestId,
        merchantId: merchantContext.merchantId,
        method,
        path,
        latencyMs: latency,
        outcome: 'SUCCESS',
      });

      return next();
    } catch (error: unknown) {
      const latency = Date.now() - startTime;
      const status = error instanceof HttpException ? error.getStatus() : HttpStatus.INTERNAL_SERVER_ERROR;
      const message = error instanceof Error ? error.message : String(error);

      this.logger.warn(`Authentication failed with status ${status}: ${message}`, {
        requestId,
        method,
        path,
        latencyMs: latency,
        outcome: `FAILED_WITH_STATUS_${status}`,
      });

      throw error;
    }
  }
}
