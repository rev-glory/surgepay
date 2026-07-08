import { Injectable } from '@nestjs/common';

import { OrderEntity } from '../entities/order.entity';
import { Order, OrderStatus } from '../generated/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class OrderRepository {
  constructor(private readonly prisma: PrismaService) {}

  private mapToEntity(model: Order): OrderEntity {
    return new OrderEntity(
      model.id,
      model.merchantId,
      model.amount,
      model.currency,
      model.status,
      model.reference,
      model.createdAt,
      model.updatedAt,
    );
  }

  async create(entity: OrderEntity): Promise<OrderEntity> {
    const model = await this.prisma.client.order.create({
      data: {
        id: entity.id,
        merchantId: entity.merchantId,
        amount: entity.amount,
        currency: entity.currency,
        status: entity.status,
        reference: entity.reference,
      },
    });
    return this.mapToEntity(model);
  }

  async findById(id: string): Promise<OrderEntity | null> {
    const model = await this.prisma.client.order.findUnique({
      where: { id },
    });
    if (!model) return null;
    return this.mapToEntity(model);
  }

  async findByMerchantAndReference(
    merchantId: string,
    reference: string,
  ): Promise<OrderEntity | null> {
    const model = await this.prisma.client.order.findUnique({
      where: {
        merchantId_reference: {
          merchantId,
          reference,
        },
      },
    });
    if (!model) return null;
    return this.mapToEntity(model);
  }

  async findByReferenceOnly(reference: string): Promise<OrderEntity | null> {
    const model = await this.prisma.client.order.findFirst({
      where: { reference },
    });
    if (!model) return null;
    return this.mapToEntity(model);
  }

  async updateStatus(id: string, status: OrderStatus): Promise<OrderEntity> {
    const model = await this.prisma.client.order.update({
      where: { id },
      data: { status },
    });
    return this.mapToEntity(model);
  }
}
