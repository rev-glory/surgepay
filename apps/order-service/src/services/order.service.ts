import { ConflictException, Injectable } from '@nestjs/common';

import {
  DomainErrorCode,
  DomainException,
  LoggerService,
  MerchantOwnershipException,
  OrderAlreadyPaidException,
  OrderAmountMismatchException,
  OrderCurrencyMismatchException,
  OrderNotFoundException,
} from '@surgepay/common';
import { OrderEligibilityRejectedReason } from '@surgepay/events';

import { OrderEntity } from '../entities/order.entity';
import { OrderStatus } from '../generated/client';
import { OrderRepository } from '../repositories/order.repository';

/**
 * Discriminated union returned by validateOrderEligibilityById().
 * Business rejections surface as typed values, never as exceptions, which allows
 * the Kafka consumer to publish the correct result event without a try/catch.
 */
export type OrderEligibilityResult =
  | { eligible: true; order: OrderEntity }
  | { eligible: false; reason: OrderEligibilityRejectedReason; orderId: string | null };

@Injectable()
export class OrderService {
  constructor(
    private readonly orderRepository: OrderRepository,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('OrderService');
  }

  /**
   * Enforces business validation rules in order:
   * 1. Order exists (404)
   * 2. Merchant owns the order (403 mapped to NotFound for isolation)
   * 3. Amount matches exactly (422)
   * 4. Currency matches exactly (422)
   * 5. Status equals CREATED (409)
   */
  async validateOrder(params: {
    merchantId: string;
    reference: string;
    amount: number;
    currency: string;
  }): Promise<OrderEntity> {
    const { merchantId, reference, amount, currency } = params;

    this.logger.info('Received internal order validation request', {
      merchantId,
      reference,
      amount,
      currency,
    });

    // 1. Order exists
    const order = await this.orderRepository.findByMerchantAndReference(merchantId, reference);
    if (!order) {
      const globalOrder = await this.orderRepository.findByReferenceOnly(reference);
      if (globalOrder) {
        this.logger.warn('Order validation failed: Merchant mismatch (global order exists)', {
          merchantId,
          orderMerchantId: globalOrder.merchantId,
          reference,
        });
        throw new MerchantOwnershipException(reference, merchantId);
      }

      this.logger.warn('Order validation failed: Order not found', { merchantId, reference });
      throw new OrderNotFoundException(reference);
    }

    // 2. Merchant owns the order (explicit safety check)
    if (order.merchantId !== merchantId) {
      this.logger.warn('Order validation failed: Merchant mismatch', {
        merchantId,
        orderMerchantId: order.merchantId,
        reference,
      });
      throw new MerchantOwnershipException(reference, merchantId);
    }

    // 3. Amount matches exactly
    if (order.amount !== amount) {
      this.logger.warn('Order validation failed: Amount mismatch', {
        reference,
        expectedAmount: order.amount,
        requestedAmount: amount,
      });
      throw new OrderAmountMismatchException(reference, order.amount, amount);
    }

    // 4. Currency matches exactly
    if (order.currency.toUpperCase() !== currency.toUpperCase()) {
      this.logger.warn('Order validation failed: Currency mismatch', {
        reference,
        expectedCurrency: order.currency,
        requestedCurrency: currency,
      });
      throw new OrderCurrencyMismatchException(reference, order.currency, currency);
    }

    // 5. Status equals CREATED
    if (order.status !== OrderStatus.CREATED) {
      this.logger.warn('Order validation failed: Order status not CREATED', {
        reference,
        orderStatus: order.status,
      });
      if (order.status === OrderStatus.CANCELLED) {
        throw new DomainException(
          `Order with reference '${reference}' is cancelled.`,
          DomainErrorCode.ORDER_ALREADY_PAID,
          409,
          { reference }
        );
      }
      if (order.status === OrderStatus.REFUNDED) {
        throw new DomainException(
          `Order with reference '${reference}' is refunded.`,
          DomainErrorCode.ORDER_ALREADY_PAID,
          409,
          { reference }
        );
      }
      throw new OrderAlreadyPaidException(reference);
    }

    this.logger.info('Order validated successfully', {
      merchantId,
      orderId: order.id,
      reference,
      amount,
      currency,
      orderStatus: order.status,
    });

    return order;
  }

  /**
   * Validates order eligibility for asynchronous saga continuation.
   *
   * Unlike validateOrder() (which throws domain exceptions for the synchronous HTTP path),
   * this method returns a typed discriminated union. Business rejections become
   * OrderEligibilityRejectedReason values — the Kafka consumer publishes them without
   * a try/catch. Infrastructure errors (DB unavailable etc.) still throw and are handled
   * by the BaseKafkaConsumer retry/DLQ path.
   *
   * Validation rules (executed in order — first failing rule wins):
   *   1. Order exists
   *   2. Merchant owns the order (mismatch → ORDER_NOT_FOUND, no cross-merchant leakage)
   *   3. Status is CREATED
   *   4. Amount matches exactly (strict integer equality — both are Int in Prisma)
   *   5. Currency matches (case-insensitive)
   */
  async validateOrderEligibilityById(params: {
    orderId: string;
    paymentId: string;
    merchantId: string;
    amount: number;
    currency: string;
  }): Promise<OrderEligibilityResult> {
    const { orderId, paymentId, merchantId, amount, currency } = params;

    this.logger.info('Received async order eligibility validation request', {
      orderId,
      paymentId,
      merchantId,
      amount,
      currency,
    });

    // Rule 1: Order exists
    const order = await this.orderRepository.findById(orderId);
    if (!order) {
      this.logger.info('Order eligibility rejected: order not found', {
        orderId,
        paymentId,
        merchantId,
      });
      return { eligible: false, reason: OrderEligibilityRejectedReason.ORDER_NOT_FOUND, orderId: null };
    }

    // Rule 2: Merchant owns the order.
    // On mismatch, return ORDER_NOT_FOUND to prevent cross-merchant order existence leakage.
    // This mirrors the synchronous path behaviour and was approved as Q2 Option B.
    if (order.merchantId !== merchantId) {
      this.logger.warn(
        'Order eligibility rejected: merchant mismatch (surfaced as ORDER_NOT_FOUND to prevent leakage)',
        { orderId, paymentId, commandMerchantId: merchantId },
      );
      return { eligible: false, reason: OrderEligibilityRejectedReason.ORDER_NOT_FOUND, orderId: null };
    }

    // Rule 3: Status is CREATED
    if (order.status !== OrderStatus.CREATED) {
      const reason =
        order.status === OrderStatus.CANCELLED
          ? OrderEligibilityRejectedReason.ORDER_CANCELLED
          : OrderEligibilityRejectedReason.ORDER_ALREADY_PAID;

      this.logger.info('Order eligibility rejected: order status not eligible', {
        orderId,
        paymentId,
        merchantId,
        orderStatus: String(order.status),
        reason,
      });
      return { eligible: false, reason, orderId: order.id };
    }

    // Rule 4: Amount matches (strict integer equality — both values are Int in the Prisma schema)
    if (order.amount !== amount) {
      this.logger.info('Order eligibility rejected: amount mismatch', {
        orderId,
        paymentId,
        merchantId,
        orderAmount: order.amount,
        commandAmount: amount,
      });
      return { eligible: false, reason: OrderEligibilityRejectedReason.AMOUNT_MISMATCH, orderId: order.id };
    }

    // Rule 5: Currency matches (case-insensitive)
    if (order.currency.toUpperCase() !== currency.toUpperCase()) {
      this.logger.info('Order eligibility rejected: currency mismatch', {
        orderId,
        paymentId,
        merchantId,
        orderCurrency: order.currency,
        commandCurrency: currency,
      });
      return { eligible: false, reason: OrderEligibilityRejectedReason.INVALID_CURRENCY, orderId: order.id };
    }

    this.logger.info('Order eligibility confirmed', {
      orderId,
      paymentId,
      merchantId,
      amount,
      currency,
    });

    return { eligible: true, order };
  }

  async createOrder(params: {
    merchantId: string;
    amount: number;
    currency: string;
    reference: string;
  }): Promise<OrderEntity> {
    const existing = await this.orderRepository.findByMerchantAndReference(
      params.merchantId,
      params.reference,
    );
    if (existing) {
      throw new ConflictException(`Order with reference '${params.reference}' already exists.`);
    }

    const order = OrderEntity.create(params);
    return this.orderRepository.create(order);
  }

  async getOrder(id: string): Promise<OrderEntity | null> {
    return this.orderRepository.findById(id);
  }
}
