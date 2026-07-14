import { HttpException, HttpStatus } from '@nestjs/common';

export interface ExceptionDetails {
  service: string;
  method: string;
  url: string;
  originalError?: string;
  statusCode?: number;
}

export class InternalServiceException extends HttpException {
  constructor(
    message: string,
    status: HttpStatus,
    public readonly details: ExceptionDetails,
  ) {
    super(
      {
        statusCode: status,
        message,
        error: HttpStatus[status] || 'InternalServiceError',
        details,
      },
      status,
    );
  }
}

export class TransportException extends InternalServiceException {
  constructor(message: string, status: HttpStatus, details: ExceptionDetails) {
    super(message, status, details);
  }
}

export class ServiceUnavailableException extends TransportException {
  constructor(message: string, details: ExceptionDetails) {
    super(message, HttpStatus.SERVICE_UNAVAILABLE, details);
  }
}

export class RequestTimeoutException extends TransportException {
  constructor(message: string, details: ExceptionDetails) {
    super(message, HttpStatus.GATEWAY_TIMEOUT, details); // 504 Gateway Timeout represents inter-service timeout
  }
}

export class DownstreamResponseException extends InternalServiceException {
  constructor(
    public readonly statusCode: number,
    public readonly responseData: unknown,
    public readonly responseHeaders: Record<string, string>,
    details: Omit<ExceptionDetails, 'statusCode' | 'originalError'>,
  ) {
    super(
      `Downstream service [${details.service}] returned status code ${statusCode}`,
      statusCode,
      {
        ...details,
        statusCode,
        originalError:
          typeof responseData === 'object' ? JSON.stringify(responseData) : String(responseData),
      },
    );
  }
}
