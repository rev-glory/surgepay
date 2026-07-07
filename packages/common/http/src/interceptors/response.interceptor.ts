import axios, { type AxiosRequestConfig, type AxiosResponse } from 'axios';

import type { LoggerService, RequestContextService } from '@surgepay/common';

import {
  DownstreamResponseException,
  RequestTimeoutException,
  ServiceUnavailableException,
} from '../errors';

export class ResponseInterceptor {
  /**
   * Logs a successful outgoing HTTP call.
   */
  static logSuccess(
    serviceName: string,
    config: AxiosRequestConfig,
    response: AxiosResponse,
    durationMs: number,
    requestContext: RequestContextService,
    logger: LoggerService,
  ): void {
    const headers = (config.headers || {}) as Record<string, string>;
    const requestId = (headers['X-Request-ID'] || requestContext.requestId || 'N/A') as string;
    const correlationId = (headers['X-Correlation-ID'] || requestContext.correlationId || 'N/A') as string;
    const merchantId = (headers['X-Merchant-ID'] || requestContext.merchantId || 'N/A') as string;

    logger.info(`Outgoing HTTP request to [${serviceName}] succeeded`, {
      destinationService: serviceName,
      method: config.method?.toUpperCase(),
      url: `${config.baseURL || ''}${config.url || ''}`,
      durationMs,
      requestId,
      correlationId,
      merchantId,
      statusCode: response.status,
    });
  }

  /**
   * Translates a transport or response failure into a standard exception and logs it.
   */
  static translateAndLogFailure(
    serviceName: string,
    config: AxiosRequestConfig,
    error: unknown,
    durationMs: number,
    requestContext: RequestContextService,
    logger: LoggerService,
  ): Error {
    const headers = (config.headers || {}) as Record<string, string>;
    const requestId = (headers['X-Request-ID'] || requestContext.requestId || 'N/A') as string;
    const correlationId = (headers['X-Correlation-ID'] || requestContext.correlationId || 'N/A') as string;
    const merchantId = (headers['X-Merchant-ID'] || requestContext.merchantId || 'N/A') as string;
    const method = (config.method || 'GET').toUpperCase();
    const fullUrl = `${config.baseURL || ''}${config.url || ''}`;

    let translated: Error;
    let statusCode: number | undefined;
    let errorMessage = String(error);

    if (axios.isAxiosError(error)) {
      errorMessage = error.message;
      const details = {
        service: serviceName,
        method,
        url: fullUrl,
        originalError: error.message,
      };

      if (error.response) {
        statusCode = error.response.status;
        translated = new DownstreamResponseException(
          error.response.status,
          error.response.data,
          error.response.headers as Record<string, string>,
          details,
        );
      } else if (error.request) {
        const code = error.code;
        if (code === 'ETIMEDOUT' || code === 'ECONNABORTED' || error.message?.includes('timeout')) {
          translated = new RequestTimeoutException(`Request to [${serviceName}] timed out after ${config.timeout}ms`, details);
        } else {
          translated = new ServiceUnavailableException(`Service [${serviceName}] is unavailable (network error: ${code || 'UNKNOWN'})`, details);
        }
      } else {
        translated = new ServiceUnavailableException(`Request setup error to [${serviceName}]: ${error.message}`, details);
      }
    } else {
      const err = error instanceof Error ? error : new Error(String(error));
      errorMessage = err.message;
      const details = {
        service: serviceName,
        method,
        url: fullUrl,
        originalError: err.message,
      };
      translated = new ServiceUnavailableException(`Request error to [${serviceName}]: ${err.message}`, details);
    }

    logger.error(`Outgoing HTTP request to [${serviceName}] failed`, undefined, {
      destinationService: serviceName,
      method,
      url: fullUrl,
      durationMs,
      requestId,
      correlationId,
      merchantId,
      statusCode: statusCode || 'N/A',
      error: errorMessage,
    });

    return translated;
  }
}
