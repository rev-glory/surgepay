import {
  CallHandler,
  ExecutionContext,
  HttpException,
  HttpStatus,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { from, Observable, of, throwError } from 'rxjs';
import { catchError, mergeMap, tap } from 'rxjs/operators';

import { LoggerService } from '@surgepay/common';
import { PlatformErrorCode } from '@surgepay/contracts';

import { IdempotencyClientService } from './idempotency-client.service';

@Injectable()
export class IdempotencyInterceptor implements NestInterceptor {
  constructor(
    private readonly idempotencyClient: IdempotencyClientService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('IdempotencyInterceptor');
  }

  async intercept(context: ExecutionContext, next: CallHandler): Promise<Observable<unknown>> {
    const httpCtx = context.switchToHttp();
    const req = httpCtx.getRequest<Request>();
    const res = httpCtx.getResponse<Response>();

    // 1. Check if the HTTP method is mutating (POST, PUT, PATCH, DELETE)
    const isMutating = ['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method || '');

    // 2. Retrieve and validate Idempotency-Key header (case-insensitive)
    const idempotencyKeyHeader = req.headers['idempotency-key'] || req.headers['Idempotency-Key'];
    const idempotencyKey =
      typeof idempotencyKeyHeader === 'string' ? idempotencyKeyHeader.trim() : undefined;

    if (isMutating) {
      if (!idempotencyKey) {
        throw new HttpException(
          {
            error: PlatformErrorCode.MISSING_IDEMPOTENCY_KEY,
            message: 'Missing or empty Idempotency-Key header',
          },
          HttpStatus.BAD_REQUEST,
        );
      }
    } else {
      // Non-mutating endpoints must not require the header
      return next.handle();
    }

    const merchant = req.merchant;
    if (!merchant) {
      this.logger.warn('Idempotency check skipped: Merchant context not found on request');
      return next.handle();
    }

    const merchantId = merchant.merchantId;

    try {
      // 2. Query Idempotency Service for state
      const checkResult = await this.idempotencyClient.check({
        merchantId,
        idempotencyKey,
        requestBody: req.body || {},
      });

      // 3. Replay cached response if COMPLETED
      if (checkResult.status === 'COMPLETED') {
        const statusCode = checkResult.statusCode || HttpStatus.OK;
        res.status(statusCode);

        if (checkResult.headers) {
          for (const [key, value] of Object.entries(checkResult.headers)) {
            res.setHeader(key, value);
          }
        }
        // Injected replay header
        res.setHeader('Idempotency-Replayed', 'true');

        this.logger.info('Transparently replaying completed response from cache', {
          merchantId,
          idempotencyKey,
          statusCode,
        });

        return of(checkResult.body);
      }

      // 4. MISS: Lock acquired. Proceed to handler and capture response.
      // Temporary implementation: Completion/cleanup is currently coordinated by the Gateway
      // until Payment Service integration is introduced downstream in subsequent commits.
      return next.handle().pipe(
        tap(async (responseBody) => {
          const statusCode = res.statusCode;
          
          if (statusCode < 500) {
            // Retrieve current response headers to cache
            const rawHeaders = res.getHeaders();
            const headersToCache: Record<string, string> = {};
            for (const [key, value] of Object.entries(rawHeaders)) {
              if (
                value !== undefined &&
                !['connection', 'keep-alive', 'transfer-encoding', 'date', 'x-powered-by'].includes(
                  key.toLowerCase(),
                )
              ) {
                headersToCache[key] = String(value);
              }
            }

            try {
              await this.idempotencyClient.complete({
                merchantId,
                idempotencyKey,
                ownerId: checkResult.ownerId!,
                requestHash: checkResult.requestHash!,
                statusCode,
                headers: headersToCache,
                body: responseBody ?? {},
              });
            } catch (completeErr) {
              this.logger.error('Failed to complete idempotency record in cache', completeErr);
            }
          } else {
            // Status code >= 500 signifies transient server error; release lock
            await this.idempotencyClient.cleanup({
              merchantId,
              idempotencyKey,
              ownerId: checkResult.ownerId!,
            });
          }
        }),
        catchError((err) => {
          // On route execution exception, clean up/release the lock
          return from(
            this.idempotencyClient
              .cleanup({
                merchantId,
                idempotencyKey,
                ownerId: checkResult.ownerId!,
              })
              .catch((cleanupErr) => {
                this.logger.error('Failed to clean up idempotency lock on exception', cleanupErr);
              }),
          ).pipe(mergeMap(() => throwError(() => err)));
        }),
      );
    } catch (error) {
      // Propagate HttpException (409, 422, etc.) or rethrow
      if (error instanceof HttpException) {
        throw error;
      }
      this.logger.error('Unexpected error in gateway idempotency execution flow', error);
      throw new HttpException(
        { error: 'IDEMPOTENCY_FILTER_ERROR', message: 'Failed to process request idempotency' },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
