import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import { Request, Response } from 'express';

import { BaseError } from '../errors';
import { createErrorResponse } from '../response';
import { LoggerService } from './logger.service';

@Catch()
export class ExceptionLoggingFilter implements ExceptionFilter {
  constructor(private readonly logger: LoggerService) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let code = 'INTERNAL_SERVER_ERROR';
    let message = 'Internal server error';
    let details: unknown = undefined;

    if (exception instanceof BaseError) {
      status = exception.statusCode;
      code = exception.code;
      message = exception.message;
      details = exception.details;
    } else if (exception instanceof HttpException) {
      status = exception.getStatus();
      const responseBody = exception.getResponse();
      message = exception.message;

      if (typeof responseBody === 'object' && responseBody !== null) {
        const bodyObj = responseBody as Record<string, unknown>;
        code = typeof bodyObj.error === 'string' ? bodyObj.error : 'HTTP_EXCEPTION';
        message = typeof bodyObj.message === 'string' ? bodyObj.message : exception.message;
        details = bodyObj.details || undefined;
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
      }
    );

    // Send formatted standard error response mapping to common contracts
    response.status(status).json(createErrorResponse(code, message, details));
  }
}
