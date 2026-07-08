import {
  Injectable,
  InternalServerErrorException,
  ServiceUnavailableException,
} from '@nestjs/common';

import {
  DuplicatePaymentReferenceException,
  FraudRejectedException,
  LoggerService,
  OrderAlreadyPaidException,
  OrderAmountMismatchException,
  OrderNotFoundException,
  RequestContextService,
  orderValidationDuration,
  fraudPrecheckDuration,
  paymentTransactionDuration,
} from '@surgepay/common';
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
    const requestId = this.requestContext.requestId || 'N/A';
    const correlationId = this.requestContext.correlationId || 'N/A';
    const causationId = this.requestContext.requestId || correlationId;
    const createdBy = this.requestContext.merchantId || 'MERCHANT';
    const source = 'GATEWAY';

    // 1. DTO Validation and Basic Business checks already completed.
    // 2. Local Duplicate reference check (keeps local invariants before making network calls)
    const existing = await this.paymentRepository.findByReference(merchantId, normalizedReference);
    if (existing) {
      this.logger.warn('Duplicate payment reference detected locally', {
        merchantId,
        reference: normalizedReference,
        requestId,
        correlationId,
      });
      throw new DuplicatePaymentReferenceException(merchantId, normalizedReference);
    }

    // 3. Synchronous Order Validation Call
    this.logger.info('Initiating synchronous order validation', {
      merchantId,
      reference: normalizedReference,
      requestId,
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
          mappedError = new OrderNotFoundException(normalizedReference, { cause: error });
        } else if (status === 422) {
          mappedError = new OrderAmountMismatchException(
            normalizedReference,
            body.amount,
            body.amount,
            { cause: error },
          );
        } else if (status === 409) {
          mappedError = new OrderAlreadyPaidException(normalizedReference, { cause: error });
        } else {
          mappedError = new InternalServerErrorException('Unexpected order validation failure.');
        }
      } else {
        validationResult = 'UNEXPECTED_ERROR';
        mappedError = new InternalServerErrorException('An unexpected error occurred during order validation.');
      }

      orderValidationDuration.record(durationMs, {
        merchantId,
        status: validationResult,
      });

      this.logger.info('Order validation stage latency', {
        requestId,
        correlationId,
        merchantId,
        stage: 'order-validation',
        durationMs,
      });

      this.logger.error('Order validation failed', error, {
        requestId,
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
    orderValidationDuration.record(durationMs, {
      merchantId,
      status: validationResult,
    });

    this.logger.info('Order validation stage latency', {
      requestId,
      correlationId,
      merchantId,
      stage: 'order-validation',
      durationMs,
    });

    this.logger.info('Order validation succeeded', {
      requestId,
      correlationId,
      merchantId,
      reference: normalizedReference,
      orderId,
      durationMs,
      validationResult,
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
          requestId,
          correlationId,
          merchantId,
          reference: normalizedReference,
          riskScore,
          decision: fraudDecision,
          ruleTriggered: response.reason,
          durationMs: evalDuration,
        });
        throw new FraudRejectedException(
          merchantId,
          body.amount,
          body.currency,
          riskScore,
          response.reason,
        );
      }
    } catch (error: unknown) {
      const evalDuration = Date.now() - fraudStartTime;
      let mappedError: Error;

      const finalDecision = error instanceof FraudRejectedException ? 'REJECTED' : fraudDecision;

      this.logger.info('Fraud pre-check stage latency', {
        requestId,
        correlationId,
        merchantId,
        stage: 'fraud-precheck',
        durationMs: evalDuration,
      });
      fraudPrecheckDuration.record(evalDuration, {
        merchantId,
        status: finalDecision,
      });

      if (error instanceof FraudRejectedException) {
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
          mappedError = new FraudRejectedException(
            merchantId,
            body.amount,
            body.currency,
            0,
            'Downstream fraud check failure',
            { cause: error },
          );
        } else {
          mappedError = new ServiceUnavailableException('Fraud pre-check service is unavailable.');
        }
      } else {
        fraudDecision = 'UNEXPECTED_ERROR';
        mappedError = new ServiceUnavailableException('Fraud pre-check service encountered an unexpected error.');
      }

      this.logger.error('Fraud precheck failed', error, {
        requestId,
        correlationId,
        merchantId,
        reference: normalizedReference,
        durationMs: evalDuration,
        fraudDecision,
      });

      throw mappedError;
    }

    const evalDuration = Date.now() - fraudStartTime;
    this.logger.info('Fraud pre-check stage latency', {
      requestId,
      correlationId,
      merchantId,
      stage: 'fraud-precheck',
      durationMs: evalDuration,
    });
    fraudPrecheckDuration.record(evalDuration, {
      merchantId,
      status: fraudDecision,
    });

    this.logger.info('Fraud pre-check passed', {
      requestId,
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
      requestId,
      correlationId,
      causationId,
      createdBy,
      source,
    });

    // Create the Event payload
    const eventPayload = {
      paymentId: payment.id,
      amount: payment.amount,
      currency: payment.currency,
      merchantId: payment.merchantId,
      orderId: orderId || '',
      paymentMethod: body.paymentMethod || 'card',
    };

    // Construct the Outbox aggregate root - delegates envelope construction internally
    const outboxEvent = OutboxEventEntity.create({
      aggregateId: payment.id,
      aggregateType: 'Payment',
      eventType: 'PaymentInitiated',
      payload: eventPayload,
      requestId,
      correlationId,
      causationId,
    });

    try {
      this.logger.info('Transaction Started', {
        requestId,
        correlationId,
        merchantId,
        paymentId: payment.id,
      });
    } catch (_logErr) {
      // Non-blocking logging
    }

    let persisted: PaymentEntity;
    const dbStartTime = Date.now();
    try {
      const result = await this.prismaService.client.$transaction(async (tx) => {
        const persistedPayment = await this.paymentRepository.create(payment, tx);
        try {
          this.logger.info('Payment Inserted', {
            requestId,
            correlationId,
            merchantId,
            paymentId: payment.id,
          });
        } catch (_logErr) {
          // Non-blocking logging
        }

        await this.outboxRepository.save(outboxEvent, tx);
        try {
          this.logger.info('Outbox Inserted', {
            requestId,
            correlationId,
            merchantId,
            paymentId: payment.id,
            outboxEventId: outboxEvent.id,
          });
        } catch (_logErr) {
          // Non-blocking logging
        }

        return persistedPayment;
      });
      persisted = result;
      const dbDurationMs = Date.now() - dbStartTime;

      this.logger.info('Database transaction stage latency', {
        requestId,
        correlationId,
        merchantId,
        paymentId: payment.id,
        stage: 'database-transaction',
        durationMs: dbDurationMs,
      });
      paymentTransactionDuration.record(dbDurationMs, {
        merchantId,
        status: 'success',
      });
    } catch (error: unknown) {
      const dbDurationMs = Date.now() - dbStartTime;

      this.logger.info('Database transaction stage latency', {
        requestId,
        correlationId,
        merchantId,
        paymentId: payment.id,
        stage: 'database-transaction',
        durationMs: dbDurationMs,
      });
      paymentTransactionDuration.record(dbDurationMs, {
        merchantId,
        status: 'error',
      });

      try {
        this.logger.error('Transaction Rolled Back', error instanceof Error ? error : new Error(String(error)), {
          requestId,
          correlationId,
          merchantId,
          paymentId: payment.id,
        });
      } catch (_logErr) {
        // Non-blocking logging
      }
      throw error;
    }

    try {
      this.logger.info('Transaction Committed', {
        requestId,
        correlationId,
        merchantId,
        paymentId: persisted.id,
      });
    } catch (_logErr) {
      // Non-blocking logging
    }

    // Emit structured logs on success
    try {
      this.logger.info('Payment created successfully', {
        requestId,
        correlationId,
        merchantId,
        paymentId: persisted.id,
        amount: persisted.amount,
        currency: persisted.currency,
        reference: persisted.reference,
        paymentStatus: persisted.status,
      });
    } catch (_logErr) {
      // Non-blocking logging
    }

    return persisted;
  }

  async getPayment(id: string): Promise<PaymentEntity | null> {
    this.logger.info('Retrieving payment by ID', { id });
    return this.paymentRepository.findById(id);
  }
}
