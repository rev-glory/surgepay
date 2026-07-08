import {
  ConflictException,
  InternalServerErrorException,
  NotFoundException,
  ServiceUnavailableException,
  UnprocessableEntityException,
} from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService, RequestContextService } from '@surgepay/common';
import {
  DownstreamResponseException,
  RequestTimeoutException,
  ServiceClient,
  ServiceUnavailableException as HttpServiceUnavailableException,
} from '@surgepay/common-http';

import { PaymentEntity } from '../entities/payment.entity';
import { PaymentRepository } from '../repositories/payment.repository';
import { PaymentService } from './payment.service';

describe('PaymentService', () => {
  let paymentService: PaymentService;
  let paymentRepository: jest.Mocked<PaymentRepository>;
  let mockOrderHttpClient: { post: jest.Mock };
  let mockRequestContext: jest.Mocked<RequestContextService>;

  beforeEach(async () => {
    // Setup Mock Repository
    paymentRepository = {
      findByReference: jest.fn(),
      create: jest.fn(),
      findById: jest.fn(),
    } as unknown as jest.Mocked<PaymentRepository>;

    // Setup Mock ServiceClient
    mockOrderHttpClient = {
      post: jest.fn(),
    };
    const mockServiceClient = {
      orderService: mockOrderHttpClient,
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
  };

  it('should successfully persist payment after successful synchronous order validation', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    mockOrderHttpClient.post.mockResolvedValue({ valid: true, orderId: 'test-order-uuid' });
    const mockPersisted = PaymentEntity.create({ ...mockPayload, merchantId: mockMerchantId });
    paymentRepository.create.mockResolvedValue(mockPersisted);

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
    expect(paymentRepository.create).toHaveBeenCalled();
  });

  it('should fail-fast on duplicate reference and bypass order validation and persistence', async () => {
    const mockExisting = PaymentEntity.create({ ...mockPayload, merchantId: mockMerchantId });
    paymentRepository.findByReference.mockResolvedValue(mockExisting);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(ConflictException);

    expect(mockOrderHttpClient.post).not.toHaveBeenCalled();
    expect(paymentRepository.create).not.toHaveBeenCalled();
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

    expect(paymentRepository.create).not.toHaveBeenCalled();
  });

  it('should map downstream 403 Forbidden (merchant mismatch) to NotFoundException and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    const downstreamError = new DownstreamResponseException(403, { message: 'Merchant mismatch' }, {}, {
      service: 'order-service',
      method: 'POST',
      url: '/internal/orders/validate',
    });
    mockOrderHttpClient.post.mockRejectedValue(downstreamError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(NotFoundException);

    expect(paymentRepository.create).not.toHaveBeenCalled();
  });

  it('should map downstream 422 Unprocessable Entity to UnprocessableEntityException and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    const downstreamError = new DownstreamResponseException(422, { message: 'Amount mismatch' }, {}, {
      service: 'order-service',
      method: 'POST',
      url: '/internal/orders/validate',
    });
    mockOrderHttpClient.post.mockRejectedValue(downstreamError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(UnprocessableEntityException);

    expect(paymentRepository.create).not.toHaveBeenCalled();
  });

  it('should map downstream 409 Conflict to ConflictException and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    const downstreamError = new DownstreamResponseException(409, { message: 'Order is paid' }, {}, {
      service: 'order-service',
      method: 'POST',
      url: '/internal/orders/validate',
    });
    mockOrderHttpClient.post.mockRejectedValue(downstreamError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(ConflictException);

    expect(paymentRepository.create).not.toHaveBeenCalled();
  });

  it('should map HTTP timeout errors to ServiceUnavailableException (503) and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    const timeoutError = new RequestTimeoutException('Timed out', {
      service: 'order-service',
      method: 'POST',
      url: '/internal/orders/validate',
    });
    mockOrderHttpClient.post.mockRejectedValue(timeoutError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(ServiceUnavailableException);

    expect(paymentRepository.create).not.toHaveBeenCalled();
  });

  it('should map other HTTP service errors to ServiceUnavailableException (503) and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    const serviceError = new HttpServiceUnavailableException('Unavailable', {
      service: 'order-service',
      method: 'POST',
      url: '/internal/orders/validate',
    });
    mockOrderHttpClient.post.mockRejectedValue(serviceError);

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(ServiceUnavailableException);

    expect(paymentRepository.create).not.toHaveBeenCalled();
  });

  it('should map unexpected errors to InternalServerErrorException (500) and bypass persistence', async () => {
    paymentRepository.findByReference.mockResolvedValue(null);
    mockOrderHttpClient.post.mockRejectedValue(new Error('Something blew up'));

    await expect(
      paymentService.createPayment(mockPayload, mockMerchantId),
    ).rejects.toThrow(InternalServerErrorException);

    expect(paymentRepository.create).not.toHaveBeenCalled();
  });
});
