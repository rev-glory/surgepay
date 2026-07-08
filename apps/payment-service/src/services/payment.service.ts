import { randomUUID } from 'crypto';

import {
  ConflictException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
  ServiceUnavailableException,
  UnprocessableEntityException,
} from '@nestjs/common';

import { LoggerService, PaymentBlockedError, RequestContextService } from '@surgepay/common';
import {
  DownstreamResponseException,
  RequestTimeoutException,
  ServiceClient,
  ServiceUnavailableException as HttpServiceUnavailableException,
} from '@surgepay/common-http';

import { CreatePaymentRequestDto } from '../dto/create-payment-request.dto';
import { OutboxEventEntity } from '../entities/outbox-event.entity';
import { PaymentEntity } from '../entities/payment.entity';
import { PrismaService } from '../prisma/prisma.service';
import { OutboxRepository } from '../repositories/outbox.repository';
import { PaymentRepository } from '../repositories/payment.repository';

@Injectable()
export class PaymentService {
  constructor(
    private readonly paymentRepository: PaymentRepository,
    private readonly outboxRepository: OutboxRepository,
    private readonly prismaService: PrismaService,
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

    // 3b. Synchronous Fraud Pre-check
    this.logger.info('Initiating synchronous fraud precheck', {
      correlationId,
      merchantId,
      reference: normalizedReference,
    });

    const fraudStartTime = Date.now();
    let fraudDecision = 'APPROVED';
    let riskScore = 0;

    try {
      const response = await this.serviceClient.fraudService.post<{
        approved: boolean;
        riskScore: number;
        reason?: string;
      }>(
        '/api/v1/internal/fraud/precheck',
        {
          merchantId,
          amount: body.amount,
          currency: body.currency,
        },
        {
          timeout: 2000, // strictly 2-second timeout
        },
      );

      riskScore = response.riskScore;

      if (!response.approved) {
        fraudDecision = 'REJECTED';
        const evalDuration = Date.now() - fraudStartTime;
        this.logger.warn('Payment rejected by fraud rules', {
          correlationId,
          merchantId,
          reference: normalizedReference,
          riskScore,
          decision: fraudDecision,
          ruleTriggered: response.reason,
          durationMs: evalDuration,
        });
        throw new PaymentBlockedError('Payment rejected by fraud rules');
      }
    } catch (error: unknown) {
      const evalDuration = Date.now() - fraudStartTime;
      let mappedError: Error;

      if (error instanceof PaymentBlockedError) {
        throw error;
      }

      if (error instanceof RequestTimeoutException) {
        fraudDecision = 'TIMEOUT';
        mappedError = new ServiceUnavailableException('Fraud pre-check service timed out.');
      } else if (error instanceof HttpServiceUnavailableException) {
        fraudDecision = 'SERVICE_UNAVAILABLE';
        mappedError = new ServiceUnavailableException('Fraud pre-check service is unavailable.');
      } else if (error instanceof DownstreamResponseException) {
        const status = error.getStatus();
        fraudDecision = `DOWNSTREAM_ERROR_${status}`;

        if (status === 403) {
          mappedError = new PaymentBlockedError('Payment rejected by fraud rules');
        } else {
          mappedError = new ServiceUnavailableException('Fraud pre-check service is unavailable.');
        }
      } else {
        fraudDecision = 'UNEXPECTED_ERROR';
        mappedError = new ServiceUnavailableException('Fraud pre-check service encountered an unexpected error.');
      }

      this.logger.error('Fraud precheck failed', error, {
        correlationId,
        merchantId,
        reference: normalizedReference,
        durationMs: evalDuration,
        fraudDecision,
      });

      throw mappedError;
    }

    const evalDuration = Date.now() - fraudStartTime;
    this.logger.info('Fraud pre-check passed', {
      correlationId,
      merchantId,
      reference: normalizedReference,
      riskScore,
      durationMs: evalDuration,
      fraudDecision,
    });

    // 4. Persist via repository after successful order validation and fraud precheck
    const payment = PaymentEntity.create({
      merchantId,
      amount: body.amount,
      currency: body.currency,
      reference: normalizedReference,
    });

    // Create the Event Envelope payload
    const eventPayload = {
      paymentId: payment.id,
      amount: payment.amount,
      currency: payment.currency,
      merchantId: payment.merchantId,
      orderId: orderId || '',
      paymentMethod: body.paymentMethod || 'card',
    };

    // Construct the platform standard event envelope
    const envelope = {
      eventId: randomUUID(),
      eventType: 'PaymentInitiated',
      version: 1,
      correlationId,
      causationId: this.requestContext.requestId || correlationId,
      sagaId: correlationId,
      timestamp: new Date().toISOString(),
      payload: eventPayload,
    };

    const outboxEvent = OutboxEventEntity.create({
      aggregateId: payment.id,
      aggregateType: 'Payment',
      eventType: 'PaymentInitiated',
      payload: envelope,
    });

    // Save both inside a single database transaction
    const { persisted } = await this.prismaService.client.$transaction(async (tx) => {
      const persisted = await this.paymentRepository.create(payment, tx);
      await this.outboxRepository.save(outboxEvent, tx);
      return { persisted };
    });

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
