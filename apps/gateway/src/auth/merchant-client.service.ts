import * as http from 'http';
import * as https from 'https';

import { HttpException, HttpStatus, Injectable } from '@nestjs/common';
import axios, { AxiosInstance } from 'axios';

import { LoggerService } from '@surgepay/common';
import { ValidateMerchantResponse } from '@surgepay/contracts';

import { getGatewayConfig } from '../config/gateway-config.schema';

@Injectable()
export class MerchantClientService {
  private readonly axiosInstance: AxiosInstance;

  constructor(private readonly logger: LoggerService) {
    const config = getGatewayConfig();

    const httpAgent = new http.Agent({ keepAlive: true });
    const httpsAgent = new https.Agent({ keepAlive: true });

    this.axiosInstance = axios.create({
      baseURL: config.MERCHANT_SERVICE_URL,
      timeout: config.MERCHANT_SERVICE_TIMEOUT,
      httpAgent,
      httpsAgent,
    });
  }

  /**
   * Calls the Merchant Service to validate an API key.
   *
   * @param apiKey The plain text API key.
   * @throws HttpException with 401, 403, or 503 status code.
   */
  async validateApiKey(apiKey: string): Promise<ValidateMerchantResponse> {
    try {
      this.logger.debug('Sending API key validation request to Merchant Service...');

      const response = await this.axiosInstance.get<ValidateMerchantResponse>(
        '/api/v1/internal/merchants/validate',
        {
          headers: {
            'x-api-key': apiKey,
          },
        },
      );

      const data = response.data;

      // Validate response payload shape
      if (
        !data ||
        typeof data.merchantId !== 'string' ||
        typeof data.merchantName !== 'string' ||
        !Array.isArray(data.permissions) ||
        typeof data.webhookEnabled !== 'boolean'
      ) {
        this.logger.error('Invalid response payload structure from Merchant Service', undefined, { data });
        throw new Error('Invalid response structure from Merchant Service');
      }

      return data;
    } catch (error: unknown) {
      let status: number | undefined;
      let message = String(error);

      if (axios.isAxiosError(error)) {
        status = error.response?.status;
        const responseData = error.response?.data as Record<string, unknown> | undefined;
        const errorDetails = responseData?.error as Record<string, unknown> | undefined;
        message = typeof errorDetails?.message === 'string' ? errorDetails.message : error.message;
      } else if (error instanceof Error) {
        message = error.message;
      }

      this.logger.error(
        `Merchant validation HTTP call failed. Status: ${status || 'N/A'}. Message: ${message}`,
        error instanceof Error ? error : undefined,
      );

      if (status === HttpStatus.UNAUTHORIZED) {
        throw new HttpException('Invalid API key', HttpStatus.UNAUTHORIZED);
      } else if (status === HttpStatus.FORBIDDEN) {
        throw new HttpException('Merchant status is inactive', HttpStatus.FORBIDDEN);
      }

      // Any other error (timeout, network failure, 500, etc.) translates to 503
      throw new HttpException(
        'Merchant Service is unavailable',
        HttpStatus.SERVICE_UNAVAILABLE,
      );
    }
  }
}
