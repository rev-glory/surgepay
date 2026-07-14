import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService } from '@surgepay/common';
import { OrderEligibilityRejectedReason } from '@surgepay/events';

import { OrderEntity } from '../entities/order.entity';
import { OrderStatus } from '../generated/client';
import { OrderRepository } from '../repositories/order.repository';
import { OrderService } from './order.service';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Creates a fully populated OrderEntity for test assertions. */
const makeOrder = (overrides: {
  id?: string;
  merchantId?: string;
  amount?: number;
  currency?: string;
  status?: OrderStatus;
} = {}): OrderEntity =>
  new OrderEntity(
    overrides.id ?? 'order-uuid-123',
    overrides.merchantId ?? 'merchant-uuid-456',
    overrides.amount ?? 10_000,
    overrides.currency ?? 'USD',
    overrides.status ?? OrderStatus.CREATED,
    'REF-001',
    new Date('2024-01-01T00:00:00Z'),
    new Date('2024-01-01T00:00:00Z'),
  );

/** Default command params that pass all five validation rules against makeOrder(). */
const defaultParams = {
  orderId: 'order-uuid-123',
  paymentId: 'payment-uuid-789',
  merchantId: 'merchant-uuid-456',
  amount: 10_000,
  currency: 'USD',
} as const;

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe('OrderService.validateOrderEligibilityById', () => {
  let service: OrderService;
  let repoMock: jest.Mocked<Pick<OrderRepository, 'findById'>>;
  let loggerMock: jest.Mocked<Partial<LoggerService>>;

  beforeEach(async () => {
    repoMock = {
      findById: jest.fn(),
    };
    loggerMock = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrderService,
        // Provide a partial mock of OrderRepository so only findById is needed
        { provide: OrderRepository, useValue: repoMock },
        { provide: LoggerService, useValue: loggerMock },
      ],
    }).compile();

    service = module.get<OrderService>(OrderService);
  });

  // -------------------------------------------------------------------------
  // Test 1 — Happy path
  // -------------------------------------------------------------------------
  it('should return eligible: true when all validation rules pass', async () => {
    const order = makeOrder();
    repoMock.findById.mockResolvedValue(order);

    const result = await service.validateOrderEligibilityById(defaultParams);

    expect(result).toEqual({ eligible: true, order });
    expect(repoMock.findById).toHaveBeenCalledWith('order-uuid-123');
  });

  // -------------------------------------------------------------------------
  // Test 2 — Rule 1: Order not found
  // -------------------------------------------------------------------------
  it('should return ORDER_NOT_FOUND with orderId: null when the order does not exist', async () => {
    repoMock.findById.mockResolvedValue(null);

    const result = await service.validateOrderEligibilityById(defaultParams);

    expect(result).toEqual({
      eligible: false,
      reason: OrderEligibilityRejectedReason.ORDER_NOT_FOUND,
      orderId: null,
    });
  });

  // -------------------------------------------------------------------------
  // Test 3 — Rule 2: Merchant mismatch (Q2 Option B — surfaces as ORDER_NOT_FOUND)
  // -------------------------------------------------------------------------
  it('should return ORDER_NOT_FOUND with orderId: null on merchant mismatch to prevent cross-merchant leakage', async () => {
    const order = makeOrder({ merchantId: 'different-merchant-id' });
    repoMock.findById.mockResolvedValue(order);

    const result = await service.validateOrderEligibilityById({
      ...defaultParams,
      merchantId: 'requesting-merchant-id',
    });

    expect(result).toEqual({
      eligible: false,
      reason: OrderEligibilityRejectedReason.ORDER_NOT_FOUND,
      orderId: null,
    });
    // Warn log must be emitted for observability
    expect(loggerMock.warn).toHaveBeenCalledWith(
      expect.stringContaining('merchant mismatch'),
      expect.any(Object),
    );
  });

  // -------------------------------------------------------------------------
  // Test 4 — Rule 3: CANCELLED status
  // -------------------------------------------------------------------------
  it('should return ORDER_CANCELLED when the order has been cancelled', async () => {
    const order = makeOrder({ status: OrderStatus.CANCELLED });
    repoMock.findById.mockResolvedValue(order);

    const result = await service.validateOrderEligibilityById(defaultParams);

    expect(result).toEqual({
      eligible: false,
      reason: OrderEligibilityRejectedReason.ORDER_CANCELLED,
      orderId: 'order-uuid-123',
    });
  });

  // -------------------------------------------------------------------------
  // Test 5 — Rule 3: PAID status
  // -------------------------------------------------------------------------
  it('should return ORDER_ALREADY_PAID when the order is PAID', async () => {
    const order = makeOrder({ status: OrderStatus.PAID });
    repoMock.findById.mockResolvedValue(order);

    const result = await service.validateOrderEligibilityById(defaultParams);

    expect(result).toEqual({
      eligible: false,
      reason: OrderEligibilityRejectedReason.ORDER_ALREADY_PAID,
      orderId: 'order-uuid-123',
    });
  });

  // -------------------------------------------------------------------------
  // Test 6 — Rule 3: REFUNDED status (paid then refunded → not eligible)
  // -------------------------------------------------------------------------
  it('should return ORDER_ALREADY_PAID when the order is REFUNDED', async () => {
    const order = makeOrder({ status: OrderStatus.REFUNDED });
    repoMock.findById.mockResolvedValue(order);

    const result = await service.validateOrderEligibilityById(defaultParams);

    expect(result).toEqual({
      eligible: false,
      reason: OrderEligibilityRejectedReason.ORDER_ALREADY_PAID,
      orderId: 'order-uuid-123',
    });
  });

  // -------------------------------------------------------------------------
  // Test 7 — Rule 4: Amount mismatch
  // -------------------------------------------------------------------------
  it('should return AMOUNT_MISMATCH when the command amount differs from the order amount', async () => {
    const order = makeOrder({ amount: 10_000 });
    repoMock.findById.mockResolvedValue(order);

    const result = await service.validateOrderEligibilityById({
      ...defaultParams,
      amount: 9_999, // deliberately wrong
    });

    expect(result).toEqual({
      eligible: false,
      reason: OrderEligibilityRejectedReason.AMOUNT_MISMATCH,
      orderId: 'order-uuid-123',
    });
  });

  // -------------------------------------------------------------------------
  // Test 8 — Rule 5: Currency mismatch
  // -------------------------------------------------------------------------
  it('should return INVALID_CURRENCY when the command currency differs from the order currency', async () => {
    const order = makeOrder({ currency: 'USD' });
    repoMock.findById.mockResolvedValue(order);

    const result = await service.validateOrderEligibilityById({
      ...defaultParams,
      currency: 'EUR', // deliberately wrong
    });

    expect(result).toEqual({
      eligible: false,
      reason: OrderEligibilityRejectedReason.INVALID_CURRENCY,
      orderId: 'order-uuid-123',
    });
  });

  // -------------------------------------------------------------------------
  // Test 9 — Currency comparison is case-insensitive (Rule 5)
  // -------------------------------------------------------------------------
  it('should treat currency comparison as case-insensitive', async () => {
    const order = makeOrder({ currency: 'USD' });
    repoMock.findById.mockResolvedValue(order);

    const result = await service.validateOrderEligibilityById({
      ...defaultParams,
      currency: 'usd', // lowercase — should still pass
    });

    expect(result).toEqual({ eligible: true, order });
  });

  // -------------------------------------------------------------------------
  // Test 10 — Rule ordering: missing order overrides currency mismatch (Rule 1 wins)
  // -------------------------------------------------------------------------
  it('should respect rule ordering: ORDER_NOT_FOUND before INVALID_CURRENCY', async () => {
    // Order does not exist, but command also has wrong currency — Rule 1 must win
    repoMock.findById.mockResolvedValue(null);

    const result = await service.validateOrderEligibilityById({
      ...defaultParams,
      currency: 'JPY', // would be INVALID_CURRENCY if order existed
    });

    // Rule 1 fires first — ORDER_NOT_FOUND, orderId: null
    expect(result).toEqual({
      eligible: false,
      reason: OrderEligibilityRejectedReason.ORDER_NOT_FOUND,
      orderId: null,
    });
    // findById must have been called exactly once — no further validation proceeded
    expect(repoMock.findById).toHaveBeenCalledTimes(1);
  });
});
