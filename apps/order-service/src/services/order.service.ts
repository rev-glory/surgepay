import {
  ConflictException,
  ForbiddenException,
  Injectable,
  NotFoundException,
  UnprocessableEntityException,
} from '@nestjs/common';

import { LoggerService } from '@surgepay/common';

import { OrderEntity } from '../entities/order.entity';
import { OrderStatus } from '../generated/client';
import { OrderRepository } from '../repositories/order.repository';

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
   * 2. Merchant owns the order (403)
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
        throw new ForbiddenException('Merchant does not own this order.');
      }

      this.logger.warn('Order validation failed: Order not found', { merchantId, reference });
      throw new NotFoundException(`Order with reference '${reference}' not found.`);
    }

    // 2. Merchant owns the order (explicit safety check)
    if (order.merchantId !== merchantId) {
      this.logger.warn('Order validation failed: Merchant mismatch', {
        merchantId,
        orderMerchantId: order.merchantId,
        reference,
      });
      throw new ForbiddenException('Merchant does not own this order.');
    }

    // 3. Amount matches exactly
    if (order.amount !== amount) {
      this.logger.warn('Order validation failed: Amount mismatch', {
        reference,
        expectedAmount: order.amount,
        requestedAmount: amount,
      });
      throw new UnprocessableEntityException(
        `Amount mismatch: Order amount is ${order.amount}, requested amount is ${amount}.`,
      );
    }

    // 4. Currency matches exactly
    if (order.currency.toUpperCase() !== currency.toUpperCase()) {
      this.logger.warn('Order validation failed: Currency mismatch', {
        reference,
        expectedCurrency: order.currency,
        requestedCurrency: currency,
      });
      throw new UnprocessableEntityException(
        `Currency mismatch: Order currency is ${order.currency}, requested currency is ${currency}.`,
      );
    }

    // 5. Status equals CREATED
    if (order.status !== OrderStatus.CREATED) {
      this.logger.warn('Order validation failed: Order status not CREATED', {
        reference,
        orderStatus: order.status,
      });
      if (order.status === OrderStatus.PAID) {
        throw new ConflictException('Order is already paid.');
      } else if (order.status === OrderStatus.CANCELLED) {
        throw new ConflictException('Order is cancelled.');
      } else if (order.status === OrderStatus.REFUNDED) {
        throw new ConflictException('Order is refunded.');
      }
      throw new ConflictException(`Order cannot be paid in status ${order.status}.`);
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
