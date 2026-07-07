import { HttpException, HttpStatus, Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import { DownstreamResponseException,ServiceClient } from '@surgepay/common-http';
import { ValidateMerchantResponse } from '@surgepay/contracts';

@Injectable()
export class MerchantClientService {
  constructor(
    private readonly serviceClient: ServiceClient,
    private readonly logger: LoggerService,
  ) {}

  /**
   * Calls the Merchant Service to validate an API key.
   *
   * @param apiKey The plain text API key.
   * @throws HttpException with 401, 403, or 503 status code.
   */
  async validateApiKey(apiKey: string): Promise<ValidateMerchantResponse> {
    try {
      this.logger.debug('Sending API key validation request to Merchant Service...');

      const data = await this.serviceClient.merchant.get<ValidateMerchantResponse>(
        '/api/v1/internal/merchants/validate',
        {
          headers: {
            'x-api-key': apiKey,
          },
        },
      );

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

      if (error instanceof DownstreamResponseException) {
        status = error.statusCode;
        const responseData = error.responseData as Record<string, unknown> | undefined;
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
