import { Injectable } from '@nestjs/common';

import { PaymentEntity } from '../entities/payment.entity';
import { Payment, PaymentStatus, Prisma } from '../generated/client';
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
      model.requestId,
      model.correlationId,
      model.causationId,
      model.createdBy,
      model.source,
      model.createdAt,
      model.updatedAt,
    );
  }

  async create(entity: PaymentEntity, tx?: Prisma.TransactionClient): Promise<PaymentEntity> {
    const client = tx || this.prisma.client;
    const model = await client.payment.create({
      data: {
        id: entity.id,
        merchantId: entity.merchantId,
        amount: entity.amount,
        currency: entity.currency,
        status: entity.status,
        reference: entity.reference,
        requestId: entity.requestId,
        correlationId: entity.correlationId,
        causationId: entity.causationId,
        createdBy: entity.createdBy,
        source: entity.source,
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
