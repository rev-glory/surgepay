import * as http from 'http';
import * as https from 'https';

import { HttpException, HttpStatus, Injectable, OnModuleDestroy } from '@nestjs/common';
import axios, { AxiosInstance } from 'axios';

import { LoggerService } from '@surgepay/common';

import { getGatewayConfig } from '../config/gateway-config.schema';

export interface CheckRequest {
  merchantId: string;
  idempotencyKey: string;
  requestBody: Record<string, unknown>;
}

export interface CheckResponse {
  status: 'MISS' | 'COMPLETED';
  ownerId?: string;
  requestHash?: string;
  statusCode?: number;
  headers?: Record<string, string>;
  body?: Record<string, unknown>;
}

export interface CompleteRequest {
  merchantId: string;
  idempotencyKey: string;
  ownerId: string;
  requestHash: string;
  statusCode: number;
  headers: Record<string, string>;
  body: Record<string, unknown>;
}

export interface CleanupRequest {
  merchantId: string;
  idempotencyKey: string;
  ownerId: string;
}

@Injectable()
export class IdempotencyClientService implements OnModuleDestroy {
  private readonly axiosInstance: AxiosInstance;
  private readonly httpAgent: http.Agent;
  private readonly httpsAgent: https.Agent;

  constructor(private readonly logger: LoggerService) {
    const config = getGatewayConfig();
    this.logger.setContext('IdempotencyClientService');

    this.httpAgent = new http.Agent({ keepAlive: true });
    this.httpsAgent = new https.Agent({ keepAlive: true });

    this.axiosInstance = axios.create({
      baseURL: config.IDEMPOTENCY_SERVICE_URL,
      timeout: config.IDEMPOTENCY_SERVICE_TIMEOUT,
      httpAgent: this.httpAgent,
      httpsAgent: this.httpsAgent,
    });
  }

  onModuleDestroy(): void {
    this.logger.info('Shutting down IdempotencyClientService agents...');
    this.httpAgent.destroy();
    this.httpsAgent.destroy();
  }

  async check(payload: CheckRequest): Promise<CheckResponse> {
    try {
      const response = await this.axiosInstance.post<CheckResponse>(
        '/api/v1/internal/idempotency/check',
        payload,
      );
      return response.data;
    } catch (error: unknown) {
      this.handleAxiosError(error, 'check');
    }
  }

  async complete(payload: CompleteRequest): Promise<void> {
    try {
      await this.axiosInstance.post('/api/v1/internal/idempotency/complete', payload);
    } catch (error: unknown) {
      this.handleAxiosError(error, 'complete');
    }
  }

  async cleanup(payload: CleanupRequest): Promise<void> {
    try {
      await this.axiosInstance.post('/api/v1/internal/idempotency/cleanup', payload);
    } catch (error: unknown) {
      this.handleAxiosError(error, 'cleanup');
    }
  }

  private handleAxiosError(error: unknown, operation: string): never {
    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let message = `Idempotency Service error during ${operation}`;

    if (axios.isAxiosError(error) && error.response) {
      status = error.response.status;
      const responseData = error.response.data as Record<string, unknown> | undefined;
      
      // Propagate the exact response structure if it exists
      if (responseData) {
        if (
          responseData.success === false &&
          typeof responseData.error === 'object' &&
          responseData.error !== null
        ) {
          const innerError = responseData.error as Record<string, unknown>;
          throw new HttpException(
            {
              error: innerError.code,
              message: innerError.message,
              details: innerError.details,
            },
            status,
          );
        }
        throw new HttpException(responseData, status);
      }
    } else if (error instanceof Error) {
      message = error.message;
    }

    this.logger.error(
      `Idempotency Service call failed (${operation}): ${message}`,
      error instanceof Error ? error : undefined,
    );
    throw new HttpException({ error: 'IDEMPOTENCY_SERVICE_ERROR', message }, status);
  }
}
