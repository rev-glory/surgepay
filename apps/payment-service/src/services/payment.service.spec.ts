import {
  ConflictException,
  NotFoundException,
  ServiceUnavailableException,
} from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService, PaymentBlockedError, RequestContextService } from '@surgepay/common';
import {
  DownstreamResponseException,
  RequestTimeoutException,
  ServiceClient,
  ServiceUnavailableException as HttpServiceUnavailableException,
} from '@surgepay/common-http';

import type { OutboxEventEntity } from '../entities/outbox-event.entity';
import { PaymentEntity } from '../entities/payment.entity';
import { PrismaService } from '../prisma/prisma.service';
import { OutboxRepository } from '../repositories/outbox.repository';
import { PaymentRepository } from '../repositories/payment.repository';
import { PaymentService } from './payment.service';

describe('PaymentService', () => {
  let paymentService: PaymentService;
  let paymentRepository: jest.Mocked<PaymentRepository>;
  let outboxRepository: jest.Mocked<OutboxRepository>;
  let mockOrderHttpClient: { post: jest.Mock };
  let mockFraudHttpClient: { post: jest.Mock };
  let mockRequestContext: jest.Mocked<RequestContextService>;
  let mockPrismaService: {
    client: {
      $transaction: jest.Mock;
    };
  };

  beforeEach(async () => {
    // Setup Mock Repository
    paymentRepository = {
      findByReference: jest.fn(),
      create: jest.fn(),
      findById: jest.fn(),
    } as unknown as jest.Mocked<PaymentRepository>;

    outboxRepository = {
      save: jest.fn(),
    } as unknown as jest.Mocked<OutboxRepository>;

    mockPrismaService = {
      client: {
        $transaction: jest.fn(async (cb: (tx: unknown) => Promise<unknown>) => {
          return cb({});
        }),
      },
    };

    // Setup Mock Service Clients
    mockOrderHttpClient = {
      post: jest.fn(),
    };
    mockFraudHttpClient = {
      post: jest.fn(),
    };
    
    const mockServiceClient = {
      orderService: mockOrderHttpClient,
      fraudService: mockFraudHttpClient,
    } as unknown as ServiceClient;

    // Setup Mock RequestContext
    mockRequestContext = {
      correlationId: 'test-correlation-id',
      requestId: 'test-request-id',
    } as unknown as jest.Mocked<RequestContextService>;

    // Setup Mock Logger
    const mockLogger = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    } as unknown as jest.Mocked<LoggerService>;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PaymentService,
        { provide: PaymentRepository, useValue: paymentRepository },
        { provide: OutboxRepository, useValue: outboxRepository },
        { provide: PrismaService, useValue: mockPrismaService },
        { provide: ServiceClient, useValue: mockServiceClient },
        { provide: RequestContextService, useValue: mockRequestContext },
        { provide: LoggerService, useValue: mockLogger },
      ],
    }).compile();

    paymentService = module.get<PaymentService>(PaymentService);
  });

  const mockMerchantId = 'fac6a364-ddcd-4c00-8d81-93740efe9150';
  const mockPayload = {
    amount: 5000,
    currency: 'INR',
    reference: 'ORDER-1001',
    paymentMethod: 'CREDIT_CARD',
  };

  it('should successfully persist payment after successful synchronous order validation and fraud precheck', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    mockOrderHttpClient.post.mockResolvedValue({ valid: true, orderId: 'test-order-uuid' });
    mockFraudHttpClient.post.mockResolvedValue({ approved: true, riskScore: 12 });
    
    const mockPersisted = PaymentEntity.create({ ...mockPayload, merchantId: mockMerchantId });
    paymentRepository.create.mockResolvedValue(mockPersisted);
    outboxRepository.save.mockResolvedValue({} as unknown as OutboxEventEntity);

    const result = await paymentService.createPayment(mockPayload, mockMerchantId);

    expect(result).toBeDefined();
    expect(paymentRepository.findByReference).toHaveBeenCalledWith(mockMerchantId, 'ORDER-1001');
    expect(mockOrderHttpClient.post).toHaveBeenCalledWith(
      '/api/v1/internal/orders/validate',
      {
        merchantId: mockMerchantId,
        reference: 'ORDER-1001',
        amount: 5000,
        currency: 'INR',
      },
      { timeout: 2000 },
    );
    expect(mockFraudHttpClient.post).toHaveBeenCalledWith(
      '/api/v1/internal/fraud/precheck',
      {
        merchantId: mockMerchantId,
        amount: 5000,
        currency: 'INR',
      },
      { timeout: 2000 },
    );
    expect(paymentRepository.create).toHaveBeenCalled();
    expect(outboxRepository.save).toHaveBeenCalled();
  });

  it('should fail-fast on duplicate reference and bypass order validation and persistence', async () => {
    const mockExisting = PaymentEntity.create({ ...mockPayload, merchantId: mockMerchantId });
    paymentRepository.findByReference.mockResolvedValue(mockExisting);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(ConflictException);

    expect(mockOrderHttpClient.post).not.toHaveBeenCalled();
    expect(mockFraudHttpClient.post).not.toHaveBeenCalled();
    expect(paymentRepository.create).not.toHaveBeenCalled();
    expect(outboxRepository.save).not.toHaveBeenCalled();
  });

  it('should map downstream 404 Not Found from Order Service to NotFoundException and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    const downstreamError = new DownstreamResponseException(404, { message: 'Not found' }, {}, {
      service: 'order-service',
      method: 'POST',
      url: '/internal/orders/validate',
    });
    mockOrderHttpClient.post.mockRejectedValue(downstreamError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(NotFoundException);

    expect(mockFraudHttpClient.post).not.toHaveBeenCalled();
    expect(paymentRepository.create).not.toHaveBeenCalled();
    expect(outboxRepository.save).not.toHaveBeenCalled();
  });

  it('should throw PaymentBlockedError when fraud precheck rules reject request and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    mockOrderHttpClient.post.mockResolvedValue({ valid: true, orderId: 'test-order-uuid' });
    mockFraudHttpClient.post.mockResolvedValue({ approved: false, riskScore: 96, reason: 'AMOUNT_THRESHOLD_EXCEEDED' });

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(PaymentBlockedError);

    expect(paymentRepository.create).not.toHaveBeenCalled();
    expect(outboxRepository.save).not.toHaveBeenCalled();
  });

  it('should map downstream 403 Forbidden from fraud precheck service to PaymentBlockedError and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    mockOrderHttpClient.post.mockResolvedValue({ valid: true, orderId: 'test-order-uuid' });
    
    const downstreamError = new DownstreamResponseException(403, { code: 'PAYMENT_BLOCKED' }, {}, {
      service: 'fraud-service',
      method: 'POST',
      url: '/internal/fraud/precheck',
    });
    mockFraudHttpClient.post.mockRejectedValue(downstreamError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(PaymentBlockedError);

    expect(paymentRepository.create).not.toHaveBeenCalled();
    expect(outboxRepository.save).not.toHaveBeenCalled();
  });

  it('should map HTTP timeout errors from fraud precheck to ServiceUnavailableException (503) and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    mockOrderHttpClient.post.mockResolvedValue({ valid: true, orderId: 'test-order-uuid' });

    const timeoutError = new RequestTimeoutException('Timed out', {
      service: 'fraud-service',
      method: 'POST',
      url: '/internal/fraud/precheck',
    });
    mockFraudHttpClient.post.mockRejectedValue(timeoutError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(ServiceUnavailableException);

    expect(paymentRepository.create).not.toHaveBeenCalled();
    expect(outboxRepository.save).not.toHaveBeenCalled();
  });

  it('should map HTTP service unavailable errors from fraud precheck to ServiceUnavailableException (503) and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    mockOrderHttpClient.post.mockResolvedValue({ valid: true, orderId: 'test-order-uuid' });

    const serviceUnavailableError = new HttpServiceUnavailableException('Service Unavailable', {
      service: 'fraud-service',
      method: 'POST',
      url: '/internal/fraud/precheck',
    });
    mockFraudHttpClient.post.mockRejectedValue(serviceUnavailableError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(ServiceUnavailableException);

    expect(paymentRepository.create).not.toHaveBeenCalled();
    expect(outboxRepository.save).not.toHaveBeenCalled();
  });

  it('should map other HTTP errors from fraud precheck to ServiceUnavailableException (503) and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    mockOrderHttpClient.post.mockResolvedValue({ valid: true, orderId: 'test-order-uuid' });

    const genericDownstreamError = new DownstreamResponseException(500, { message: 'Internal Error' }, {}, {
      service: 'fraud-service',
      method: 'POST',
      url: '/internal/fraud/precheck',
    });
    mockFraudHttpClient.post.mockRejectedValue(genericDownstreamError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(ServiceUnavailableException);

    expect(paymentRepository.create).not.toHaveBeenCalled();
    expect(outboxRepository.save).not.toHaveBeenCalled();
  });
});
