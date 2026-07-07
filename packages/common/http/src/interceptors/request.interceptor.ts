import type { AxiosRequestConfig } from 'axios';

import type { RequestContextService } from '@surgepay/common';

export class RequestInterceptor {
  /**
   * Automatically enriches outgoing request headers with request, correlation, and merchant IDs from request context.
   * Preserves any existing user-supplied headers.
   *
   * @param config The Axios request config object.
   * @param requestContext Service to retrieve AsyncLocalStorage trace parameters.
   */
  static propagateHeaders(
    config: AxiosRequestConfig,
    requestContext: RequestContextService,
  ): AxiosRequestConfig {
    const headers = (config.headers || {}) as Record<string, string>;

    const requestId = requestContext.requestId;
    const correlationId = requestContext.correlationId;
    const merchantId = requestContext.merchantId;

    if (requestId && !headers['x-request-id'] && !headers['X-Request-ID']) {
      headers['X-Request-ID'] = requestId;
    }
    if (correlationId && !headers['x-correlation-id'] && !headers['X-Correlation-ID']) {
      headers['X-Correlation-ID'] = correlationId;
    }
    if (merchantId && !headers['x-merchant-id'] && !headers['X-Merchant-ID']) {
      headers['X-Merchant-ID'] = merchantId;
    }

    config.headers = headers as AxiosRequestConfig['headers'];
    return config;
  }
}
