import { ConflictException, Injectable } from '@nestjs/common';

import { type SagaInstance, SagaStatus } from '../../generated/client';
import { PrismaService } from '../../prisma/prisma.service';
import { SagaInstanceEntity } from '../entities/saga-instance.entity';

@Injectable()
export class SagaRepository {
  constructor(private readonly prisma: PrismaService) {}

  private mapToEntity(model: SagaInstance): SagaInstanceEntity {
    return new SagaInstanceEntity(
      model.id,
      model.paymentId,
      model.correlationId,
      model.status,
      model.version,
      model.startedAt,
      model.completedAt,
      model.createdAt,
      model.updatedAt
    );
  }

  /**
   * Persists a newly created Saga aggregate root.
   */
  async create(entity: SagaInstanceEntity): Promise<SagaInstanceEntity> {
    const model = await this.prisma.client.sagaInstance.create({
      data: {
        id: entity.id,
        paymentId: entity.paymentId,
        correlationId: entity.correlationId,
        status: entity.status,
        version: entity.version,
        startedAt: entity.startedAt,
        completedAt: entity.completedAt,
      },
    });
    return this.mapToEntity(model);
  }

  /**
   * Finds a Saga by its primary key (Saga ID).
   */
  async findById(id: string): Promise<SagaInstanceEntity | null> {
    const model = await this.prisma.client.sagaInstance.findUnique({
      where: { id },
    });
    if (!model) return null;
    return this.mapToEntity(model);
  }

  /**
   * Finds a Saga by its unique Payment ID.
   */
  async findByPaymentId(paymentId: string): Promise<SagaInstanceEntity | null> {
    const model = await this.prisma.client.sagaInstance.findUnique({
      where: { paymentId },
    });
    if (!model) return null;
    return this.mapToEntity(model);
  }

  /**
   * Finds a Saga by its unique Correlation ID.
   */
  async findByCorrelationId(correlationId: string): Promise<SagaInstanceEntity | null> {
    const model = await this.prisma.client.sagaInstance.findUnique({
      where: { correlationId },
    });
    if (!model) return null;
    return this.mapToEntity(model);
  }

  /**
   * Updates an existing Saga aggregate state in the database using optimistic concurrency control.
   * Throws ConflictException if concurrency conflicts are detected.
   */
  async update(entity: SagaInstanceEntity): Promise<SagaInstanceEntity> {
    const updated = await this.prisma.client.sagaInstance.updateMany({
      where: {
        id: entity.id,
        version: entity.version,
      },
      data: {
        status: entity.status,
        completedAt: entity.completedAt,
        version: { increment: 1 },
      },
    });

    if (updated.count === 0) {
      throw new ConflictException(
        `Optimistic locking failure: SagaInstance with id ${entity.id} and version ${entity.version} was updated concurrently.`
      );
    }

    const model = await this.prisma.client.sagaInstance.findUnique({
      where: { id: entity.id },
    });
    if (!model) {
      throw new Error(`SagaInstance with id ${entity.id} was not found after update`);
    }

    return this.mapToEntity(model);
  }

  /**
   * Discovers all incomplete saga instances (status !== CLOSED) for recovery.
   */
  async findRecoverableSagas(): Promise<SagaInstanceEntity[]> {
    const models = await this.prisma.client.sagaInstance.findMany({
      where: {
        status: {
          not: SagaStatus.CLOSED,
        },
      },
      orderBy: {
        createdAt: 'asc',
      },
    });
    return models.map((model) => this.mapToEntity(model));
  }
}
