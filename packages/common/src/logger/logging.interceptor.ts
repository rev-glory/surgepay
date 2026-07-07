import { CallHandler, ExecutionContext, Injectable, NestInterceptor } from '@nestjs/common';
import { Request, Response } from 'express';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';

import { LoggerService } from './logger.service';

@Injectable()
export class LoggingInterceptor implements NestInterceptor {
  constructor(private readonly logger: LoggerService) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const httpContext = context.switchToHttp();
    const req = httpContext.getRequest<Request>();
    const res = httpContext.getResponse<Response>();

    const startTime = Date.now();
    const { method, url } = req;

    // Log incoming request execution
    this.logger.info(`Incoming request: ${method} ${url}`, {
      method,
      path: url,
    });

    return next.handle().pipe(
      tap({
        next: () => {
          const durationMs = Date.now() - startTime;
          const status = res.statusCode;
          this.logger.info(`Request completed: ${method} ${url} - ${status} (${durationMs}ms)`, {
            method,
            path: url,
            status,
            durationMs,
          });
        },
        error: (err: unknown) => {
          const durationMs = Date.now() - startTime;
          const status =
            err &&
            typeof err === 'object' &&
            'status' in err &&
            typeof (err as Record<string, unknown>).status === 'number'
              ? ((err as Record<string, unknown>).status as number)
              : 500;
          this.logger.error(`Request failed: ${method} ${url} - ${status} (${durationMs}ms)`, err, {
            method,
            path: url,
            status,
            durationMs,
          });
        },
      }),
    );
  }
}
