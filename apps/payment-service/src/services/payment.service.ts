import {
  ConflictException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
  ServiceUnavailableException,
  UnprocessableEntityException,
} from '@nestjs/common';

import { LoggerService, RequestContextService } from '@surgepay/common';
import {
  DownstreamResponseException,
  RequestTimeoutException,
  ServiceClient,
  ServiceUnavailableException as HttpServiceUnavailableException,
} from '@surgepay/common-http';

import { CreatePaymentRequestDto } from '../dto/create-payment-request.dto';
import { PaymentEntity } from '../entities/payment.entity';
import { PaymentRepository } from '../repositories/payment.repository';

@Injectable()
export class PaymentService {
  constructor(
    private readonly paymentRepository: PaymentRepository,
    private readonly serviceClient: ServiceClient,
    private readonly requestContext: RequestContextService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('PaymentService');
  }

  async createPayment(body: CreatePaymentRequestDto, merchantId: string): Promise<PaymentEntity> {
    const normalizedReference = body.reference.trim();

    // 1. DTO Validation and Basic Business checks already completed.
    // 2. Local Duplicate reference check (keeps local invariants before making network calls)
    const existing = await this.paymentRepository.findByReference(merchantId, normalizedReference);
    if (existing) {
      this.logger.warn('Duplicate payment reference detected locally', {
        merchantId,
        reference: normalizedReference,
      });
      throw new ConflictException(`Payment with reference '${normalizedReference}' already exists for this merchant.`);
    }

    // 3. Synchronous Order Validation Call
    const correlationId = this.requestContext.correlationId || 'N/A';
    this.logger.info('Initiating synchronous order validation', {
      merchantId,
      reference: normalizedReference,
      correlationId,
    });

    const startTime = Date.now();
    let orderId: string | undefined;
    let validationResult = 'PENDING';

    try {
      const response = await this.serviceClient.orderService.post<{ valid: boolean; orderId: string }>(
        '/api/v1/internal/orders/validate',
        {
          merchantId,
          reference: normalizedReference,
          amount: body.amount,
          currency: body.currency,
        },
        {
          timeout: 2000, // strictly 2-second timeout
        },
      );

      orderId = response.orderId;
      validationResult = 'SUCCESS';
    } catch (error: unknown) {
      const durationMs = Date.now() - startTime;
      let mappedError: Error;

      if (error instanceof RequestTimeoutException) {
        validationResult = 'TIMEOUT';
        mappedError = new ServiceUnavailableException('Order validation service timed out.');
      } else if (error instanceof HttpServiceUnavailableException) {
        validationResult = 'SERVICE_UNAVAILABLE';
        mappedError = new ServiceUnavailableException('Order validation service is unavailable.');
      } else if (error instanceof DownstreamResponseException) {
        const status = error.getStatus();
        validationResult = `DOWNSTREAM_ERROR_${status}`;

        if (status === 404 || status === 403) {
          // Map 403 (merchant mismatch) to 404 (not found) to avoid leaking order existence
          mappedError = new NotFoundException(`Order with reference '${normalizedReference}' not found.`);
        } else if (status === 422) {
          mappedError = new UnprocessableEntityException(
            error.message || 'Order amount or currency mismatch.',
          );
        } else if (status === 409) {
          mappedError = new ConflictException(error.message || 'Order already processed or cancelled.');
        } else {
          mappedError = new InternalServerErrorException('Unexpected order validation failure.');
        }
      } else {
        validationResult = 'UNEXPECTED_ERROR';
        mappedError = new InternalServerErrorException('An unexpected error occurred during order validation.');
      }

      this.logger.error('Order validation failed', error, {
        correlationId,
        merchantId,
        reference: normalizedReference,
        durationMs,
        validationResult,
      });

      // Fail-fast and bypass payment persistence
      throw mappedError;
    }

    const durationMs = Date.now() - startTime;
    this.logger.info('Order validation succeeded', {
      correlationId,
      merchantId,
      reference: normalizedReference,
      orderId,
      durationMs,
      validationResult,
    });

    // 4. Persist via repository after successful order validation
    const payment = PaymentEntity.create({
      merchantId,
      amount: body.amount,
      currency: body.currency,
      reference: normalizedReference,
    });

    const persisted = await this.paymentRepository.create(payment);

    // Emit structured logs on success
    this.logger.info('Payment created successfully', {
      merchantId,
      paymentId: persisted.id,
      amount: persisted.amount,
      currency: persisted.currency,
      reference: persisted.reference,
      paymentStatus: persisted.status,
    });

    return persisted;
  }

  async getPayment(id: string): Promise<PaymentEntity | null> {
    this.logger.info('Retrieving payment by ID', { id });
    return this.paymentRepository.findById(id);
  }
}
