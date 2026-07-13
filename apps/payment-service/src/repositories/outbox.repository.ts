import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';

import { OutboxEventEntity } from '../entities/outbox-event.entity';
import { OutboxEvent, OutboxStatus, Prisma } from '../generated/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class OutboxRepository {
  constructor(
    private readonly prisma: PrismaService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('OutboxRepository');
  }

  private mapToEntity(model: OutboxEvent): OutboxEventEntity {
    return new OutboxEventEntity(
      model.id,
      model.aggregateId,
      model.aggregateType,
      model.eventType,
      model.payload as Record<string, unknown>,
      model.status,
      model.requestId,
      model.correlationId,
      model.causationId,
      model.createdAt,
      model.publishedAt,
      model.retryCount,
      model.traceHeaders as Record<string, string> | null,
    );
  }

  async save(entity: OutboxEventEntity, tx?: Prisma.TransactionClient): Promise<OutboxEventEntity> {
    const client = tx || this.prisma.client;
    const model = await client.outboxEvent.create({
      data: {
        id: entity.id,
        aggregateId: entity.aggregateId,
        aggregateType: entity.aggregateType,
        eventType: entity.eventType,
        payload: entity.payload as Prisma.InputJsonValue,
        status: entity.status,
        requestId: entity.requestId,
        correlationId: entity.correlationId,
        causationId: entity.causationId,
        createdAt: entity.createdAt,
        publishedAt: entity.publishedAt,
        retryCount: entity.retryCount,
        traceHeaders: (entity.traceHeaders || {}) as Prisma.InputJsonValue,
      },
    });

    const mapped = this.mapToEntity(model);

    this.logger.info('Outbox record persisted successfully', {
      outboxEventId: mapped.id,
      aggregateId: mapped.aggregateId,
      aggregateType: mapped.aggregateType,
      eventType: mapped.eventType,
      correlationId: mapped.correlationId,
      requestId: mapped.requestId,
      causationId: mapped.causationId,
      outboxStatus: mapped.status,
      timestamp: mapped.createdAt.toISOString(),
    });

    return mapped;
  }

  async findPending(limit = 100): Promise<OutboxEventEntity[]> {
    const models = await this.prisma.client.outboxEvent.findMany({
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

  async markPublished(id: string): Promise<OutboxEventEntity> {
    const model = await this.prisma.client.outboxEvent.update({
      where: { id },
      data: {
        status: OutboxStatus.PUBLISHED,
        publishedAt: new Date(),
      },
    });
    return this.mapToEntity(model);
  }

  async incrementRetry(id: string): Promise<OutboxEventEntity> {
    const model = await this.prisma.client.outboxEvent.update({
      where: { id },
      data: {
        retryCount: {
          increment: 1,
        },
        status: OutboxStatus.RETRYING,
      },
    });
    return this.mapToEntity(model);
  }

  async markFailed(id: string): Promise<OutboxEventEntity> {
    const model = await this.prisma.client.outboxEvent.update({
      where: { id },
      data: {
        status: OutboxStatus.FAILED,
      },
    });
    return this.mapToEntity(model);
  }
}
