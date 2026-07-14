import { Injectable } from '@nestjs/common';
import { LoggerService } from '@surgepay/common';
import { OrderOutboxEventEntity } from '../entities/order-outbox-event.entity';
import { OrderOutboxEvent, OutboxStatus, Prisma } from '../../generated/client';
import { PrismaService } from '../../prisma/prisma.service';

@Injectable()
export class OrderOutboxRepository {
  constructor(
    private readonly prisma: PrismaService,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('OrderOutboxRepository');
  }

  private mapToEntity(model: OrderOutboxEvent): OrderOutboxEventEntity {
    return new OrderOutboxEventEntity(
      model.id,
      model.eventType,
      model.payload as Record<string, any>,
      model.status,
      model.createdAt,
      model.updatedAt
    );
  }

  async save(entity: OrderOutboxEventEntity, tx?: Prisma.TransactionClient): Promise<OrderOutboxEventEntity> {
    const client = tx || this.prisma.client;
    const model = await client.orderOutboxEvent.create({
      data: {
        id: entity.id,
        eventType: entity.eventType,
        payload: entity.payload as Prisma.InputJsonValue,
        status: entity.status,
        createdAt: entity.createdAt,
      },
    });

    return this.mapToEntity(model);
  }

  async findPending(limit = 50): Promise<OrderOutboxEventEntity[]> {
    const models = await this.prisma.client.orderOutboxEvent.findMany({
      where: {
        status: {
          in: [OutboxStatus.PENDING, OutboxStatus.RETRYING],
        },
      },
      orderBy: {
        createdAt: 'asc',
      },
      take: limit,
    });
    return models.map((m) => this.mapToEntity(m));
  }

  async markPublished(id: string): Promise<OrderOutboxEventEntity> {
    const model = await this.prisma.client.orderOutboxEvent.update({
      where: { id },
      data: {
        status: OutboxStatus.PUBLISHED,
      },
    });
    return this.mapToEntity(model);
  }

  async markFailed(id: string): Promise<OrderOutboxEventEntity> {
    const model = await this.prisma.client.orderOutboxEvent.update({
      where: { id },
      data: {
        status: OutboxStatus.FAILED,
      },
    });
    return this.mapToEntity(model);
  }
}
