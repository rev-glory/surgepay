import { ArgumentsHost, Catch, ExceptionFilter, HttpException, HttpStatus } from '@nestjs/common';
import { Request, Response } from 'express';

import { PlatformErrorCode, ValidationErrorDetail } from '@surgepay/contracts';

import { BaseError } from '../errors';
import { createErrorResponse } from '../response';
import { LoggerService } from './logger.service';
import { RequestContext } from './request-context';

@Catch()
export class ExceptionLoggingFilter implements ExceptionFilter {
  constructor(private readonly logger: LoggerService) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let code = PlatformErrorCode.INTERNAL_ERROR;
    let message = 'Internal server error';
    let validationErrors: ValidationErrorDetail[] | undefined = undefined;
    let metadata: Record<string, unknown> | undefined = undefined;

    if (exception instanceof BaseError) {
      status = exception.statusCode;
      code = exception.code as PlatformErrorCode;
      message = exception.message;
      if (code === PlatformErrorCode.VALIDATION_FAILED && Array.isArray(exception.details)) {
        validationErrors = exception.details as ValidationErrorDetail[];
      } else if (exception.details) {
        metadata = typeof exception.details === 'object' ? (exception.details as Record<string, unknown>) : { details: exception.details };
      }
    } else if (exception instanceof HttpException) {
      status = exception.getStatus();
      let responseBody = exception.getResponse();
      const objException = exception as unknown as Record<string, unknown>;
      if (objException && typeof objException === 'object' && 'responseData' in objException && objException.responseData) {
        responseBody = objException.responseData;
      }
      message = exception.message;

      if (typeof responseBody === 'object' && responseBody !== null) {
        const bodyObj = responseBody as Record<string, unknown>;
        
        // Extract error code from HttpException payload, checking nested structure
        let rawError = bodyObj.error || bodyObj.code;
        if (rawError && typeof rawError === 'object') {
          rawError = (rawError as Record<string, unknown>).code;
        }

        if (typeof rawError === 'string' && Object.values(PlatformErrorCode).includes(rawError as PlatformErrorCode)) {
          code = rawError as PlatformErrorCode;
        } else if (typeof bodyObj.message === 'string' && Object.values(PlatformErrorCode).includes(bodyObj.message as PlatformErrorCode)) {
          code = bodyObj.message as PlatformErrorCode;
        } else {
          // Map default HTTP status codes to standard codes if they are generic
          if (status === HttpStatus.UNAUTHORIZED) {
            code = PlatformErrorCode.INVALID_API_KEY;
          } else if (status === HttpStatus.FORBIDDEN) {
            code = PlatformErrorCode.MERCHANT_DISABLED;
          } else if (status === HttpStatus.TOO_MANY_REQUESTS) {
            code = PlatformErrorCode.RATE_LIMIT_EXCEEDED;
          } else if (status === HttpStatus.BAD_REQUEST) {
            code = PlatformErrorCode.INVALID_REQUEST;
          } else if (status === HttpStatus.CONFLICT || status === HttpStatus.UNPROCESSABLE_ENTITY) {
            code = PlatformErrorCode.IDEMPOTENCY_CONFLICT;
          }
        }

        let extractedMessage = bodyObj.message;
        if (bodyObj.error && typeof bodyObj.error === 'object') {
          const errObj = bodyObj.error as Record<string, unknown>;
          if (typeof errObj.message === 'string') {
            extractedMessage = errObj.message;
          }
        }

        if (typeof extractedMessage === 'string') {
          message = extractedMessage;
        } else if (Array.isArray(extractedMessage)) {
          message = extractedMessage.join(', ');
        }
        
        if (bodyObj.validationErrors && Array.isArray(bodyObj.validationErrors)) {
          validationErrors = bodyObj.validationErrors as ValidationErrorDetail[];
        }
        
        if (bodyObj.metadata && typeof bodyObj.metadata === 'object') {
          metadata = bodyObj.metadata as Record<string, unknown>;
        }
      } else if (typeof responseBody === 'string') {
        message = responseBody;
      }
    } else if (exception instanceof Error) {
      message = exception.message;
    }

    // Log the unhandled exception using the structured logger
    const errorLogMsg = `Unhandled exception on ${request.method} ${request.url}: ${message}`;
    this.logger.error(
      errorLogMsg,
      exception instanceof Error ? exception : new Error(String(exception)),
      {
        method: request.method,
        path: request.url,
        status,
      },
    );

    // Retrieve correlation and request identifiers
    const correlationId = RequestContext.correlationId || (request.headers['x-correlation-id'] as string | undefined);
    const requestId = RequestContext.requestId || (request.headers['x-request-id'] as string | undefined);

    // Send formatted standard error response mapping to common contracts
    response.status(status).json(
      createErrorResponse(
        code,
        message,
        status,
        request.url,
        correlationId,
        requestId,
        validationErrors,
        metadata,
      ),
    );
  }
}
