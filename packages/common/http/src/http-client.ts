import * as http from 'http';
import * as https from 'https';

import axios, { type AxiosInstance, type AxiosRequestConfig } from 'axios';

import type { LoggerService, RequestContextService } from '@surgepay/common';

import { RequestInterceptor } from './interceptors/request.interceptor';
import { ResponseInterceptor } from './interceptors/response.interceptor';
import { RetryPolicy } from './retry/retry.policy';

export class HttpClient {
  private readonly axiosInstance: AxiosInstance;
  private readonly retryPolicy: RetryPolicy;

  constructor(
    public readonly serviceName: string,
    public readonly baseURL: string,
    private readonly timeout: number,
    private readonly retries: number,
    private readonly retryDelay: number,
    private readonly requestContext: RequestContextService,
    private readonly logger: LoggerService,
  ) {
    const keepAlive = process.env.HTTP_KEEP_ALIVE !== 'false';
    const maxSockets = process.env.HTTP_KEEP_ALIVE_MAX_SOCKETS
      ? parseInt(process.env.HTTP_KEEP_ALIVE_MAX_SOCKETS, 10)
      : 100;

    const httpAgent = new http.Agent({
      keepAlive,
      maxSockets,
    });
    const httpsAgent = new https.Agent({
      keepAlive,
      maxSockets,
    });

    this.axiosInstance = axios.create({
      baseURL,
      timeout,
      httpAgent,
      httpsAgent,
    });
    this.retryPolicy = new RetryPolicy(this.retries, this.retryDelay, this.logger);
  }

  /**
   * Generic request runner executing the Axios request wrapped inside a retry policy, timing logs, and error translation interceptors.
   */
  async request<T>(
    method: string,
    url: string,
    config: AxiosRequestConfig & { forceRetry?: boolean } = {},
  ): Promise<T> {
    const startTime = Date.now();

    // 1. Prepare and enrich request configuration (propagate headers, configure timeout)
    let enrichedConfig: AxiosRequestConfig = {
      ...config,
      method,
      url,
      baseURL: this.baseURL,
      timeout: config.timeout !== undefined ? config.timeout : this.timeout,
    };

    enrichedConfig = RequestInterceptor.propagateHeaders(enrichedConfig, this.requestContext);

    try {
      // 2. Execute with retry policy
      const response = await this.retryPolicy.execute(
        () => this.axiosInstance.request(enrichedConfig),
        {
          method,
          forceRetry: config.forceRetry,
        },
      );

      const durationMs = Date.now() - startTime;

      // 3. Log success
      ResponseInterceptor.logSuccess(
        this.serviceName,
        enrichedConfig,
        response,
        durationMs,
        this.requestContext,
        this.logger,
      );

      return response.data;
    } catch (error: unknown) {
      const durationMs = Date.now() - startTime;

      // 4. Translate, log, and throw normalized exception
      const translated = ResponseInterceptor.translateAndLogFailure(
        this.serviceName,
        enrichedConfig,
        error,
        durationMs,
        this.requestContext,
        this.logger,
      );
      throw translated;
    }
  }

  async get<T>(url: string, config?: AxiosRequestConfig & { forceRetry?: boolean }): Promise<T> {
    return this.request<T>('GET', url, config);
  }

  async post<T>(
    url: string,
    data?: unknown,
    config?: AxiosRequestConfig & { forceRetry?: boolean },
  ): Promise<T> {
    return this.request<T>('POST', url, { ...config, data });
  }

  async put<T>(
    url: string,
    data?: unknown,
    config?: AxiosRequestConfig & { forceRetry?: boolean },
  ): Promise<T> {
    return this.request<T>('PUT', url, { ...config, data });
  }

  async patch<T>(
    url: string,
    data?: unknown,
    config?: AxiosRequestConfig & { forceRetry?: boolean },
  ): Promise<T> {
    return this.request<T>('PATCH', url, { ...config, data });
  }

  async delete<T>(url: string, config?: AxiosRequestConfig & { forceRetry?: boolean }): Promise<T> {
    return this.request<T>('DELETE', url, config);
  }
}
