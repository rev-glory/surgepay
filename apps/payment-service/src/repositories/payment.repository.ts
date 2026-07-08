import { Injectable } from '@nestjs/common';

import { PaymentEntity } from '../entities/payment.entity';
import { Payment, PaymentStatus } from '../generated/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class PaymentRepository {
  constructor(private readonly prisma: PrismaService) {}

  private mapToEntity(model: Payment): PaymentEntity {
    return new PaymentEntity(
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

  async create(entity: PaymentEntity): Promise<PaymentEntity> {
    const model = await this.prisma.client.payment.create({
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

  async findById(id: string): Promise<PaymentEntity | null> {
    const model = await this.prisma.client.payment.findUnique({
      where: { id },
    });
    if (!model) return null;
    return this.mapToEntity(model);
  }

  async findByReference(merchantId: string, reference: string): Promise<PaymentEntity | null> {
    const model = await this.prisma.client.payment.findUnique({
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

  async updateStatus(id: string, status: PaymentStatus): Promise<PaymentEntity> {
    const model = await this.prisma.client.payment.update({
      where: { id },
      data: { status },
    });
    return this.mapToEntity(model);
  }
}
