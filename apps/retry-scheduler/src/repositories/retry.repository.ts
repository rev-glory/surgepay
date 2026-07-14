import { Injectable } from '@nestjs/common';
import { LoggerService } from '@surgepay/common';
import { ScheduledRetryEntity } from '../entities/scheduled-retry.entity';
import { ScheduledRetry, RetryStatus, Prisma } from '../generated/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class RetryRepository {
  constructor(
    private readonly prisma: PrismaService,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('RetryRepository');
  }

  private mapToEntity(model: ScheduledRetry): ScheduledRetryEntity {
    return new ScheduledRetryEntity(
      model.id,
      model.originalTopic,
      model.originalMessage as Record<string, any>,
      model.retryCount,
      model.maxAttempts,
      model.baseDelayMs,
      model.maxDelayMs,
      model.correlationId,
      model.causationId,
      model.sagaId,
      model.executeAt,
      model.status,
      model.createdAt,
      model.updatedAt
    );
  }

  async save(entity: ScheduledRetryEntity): Promise<ScheduledRetryEntity> {
    const model = await this.prisma.client.scheduledRetry.create({
      data: {
        id: entity.id,
        originalTopic: entity.originalTopic,
        originalMessage: entity.originalMessage as Prisma.InputJsonValue,
        retryCount: entity.retryCount,
        maxAttempts: entity.maxAttempts,
        baseDelayMs: entity.baseDelayMs,
        maxDelayMs: entity.maxDelayMs,
        correlationId: entity.correlationId,
        causationId: entity.causationId,
        sagaId: entity.sagaId,
        executeAt: entity.executeAt,
        status: entity.status,
      },
    });
    return this.mapToEntity(model);
  }

  async findById(id: string): Promise<ScheduledRetryEntity | null> {
    const model = await this.prisma.client.scheduledRetry.findUnique({
      where: { id },
    });
    if (!model) return null;
    return this.mapToEntity(model);
  }

  async findDue(now: Date, limit = 50): Promise<ScheduledRetryEntity[]> {
    const models = await this.prisma.client.scheduledRetry.findMany({
      where: {
        status: RetryStatus.PENDING,
        executeAt: {
          lte: now,
        },
      },
      orderBy: {
        executeAt: 'asc',
      },
      take: limit,
    });
    return models.map((m) => this.mapToEntity(m));
  }

  async markExecuted(id: string): Promise<ScheduledRetryEntity> {
    const model = await this.prisma.client.scheduledRetry.update({
      where: { id },
      data: {
        status: RetryStatus.EXECUTED,
      },
    });
    return this.mapToEntity(model);
  }

  async markExhausted(id: string): Promise<ScheduledRetryEntity> {
    const model = await this.prisma.client.scheduledRetry.update({
      where: { id },
      data: {
        status: RetryStatus.EXHAUSTED,
      },
    });
    return this.mapToEntity(model);
  }
}
