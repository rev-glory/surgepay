import { ConflictException, Injectable } from '@nestjs/common';

import {
  OrderValidationStatus,
  type SagaInstance,
  SagaStatus,
  SagaTransitionType,
} from '../../generated/client';
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
      model.orderValidationStatus,
      model.merchantId,
      model.amount,
      model.currency,
      model.version,
      model.startedAt,
      model.completedAt,
      model.createdAt,
      model.updatedAt,
      model.failureReason,
      model.failedAt,
      model.originService,
      model.stateUpdatedAt,
      model.retryCount,
      model.lastRetryAt,
      model.nextRetryAt,
      model.currentCommandId,
      model.retryHandoffAt
    );
  }

  /**
   * Persists a newly created Saga aggregate root and initial transition(s) inside a transaction.
   */
  async create(
    entity: SagaInstanceEntity,
    transitions?: {
      transitionType: SagaTransitionType;
      fromState: string;
      toState: string;
      eventId: string;
      causationId: string;
      eventType: string;
    }[]
  ): Promise<SagaInstanceEntity> {
    return this.prisma.client.$transaction(async (tx) => {
      const model = await tx.sagaInstance.create({
        data: {
          id: entity.id,
          paymentId: entity.paymentId,
          correlationId: entity.correlationId,
          status: entity.status,
          orderValidationStatus: entity.orderValidationStatus,
          merchantId: entity.merchantId,
          amount: entity.amount,
          currency: entity.currency,
          version: entity.version,
          startedAt: entity.startedAt,
          completedAt: entity.completedAt,
          failureReason: entity.failureReason,
          failedAt: entity.failedAt,
          originService: entity.originService,
          stateUpdatedAt: entity.stateUpdatedAt,
          retryCount: entity.retryCount,
          lastRetryAt: entity.lastRetryAt,
          nextRetryAt: entity.nextRetryAt,
          currentCommandId: entity.currentCommandId,
          retryHandoffAt: entity.retryHandoffAt,
        },
      });

      if (transitions && transitions.length > 0) {
        for (const t of transitions) {
          await tx.sagaTransition.create({
            data: {
              sagaId: entity.id,
              correlationId: entity.correlationId,
              transitionType: t.transitionType,
              fromState: t.fromState,
              toState: t.toState,
              eventId: t.eventId,
              causationId: t.causationId,
              eventType: t.eventType,
            },
          });
        }
      }

      return this.mapToEntity(model);
    });
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
   * Persists explicit transition audit records within the same transaction.
   */
  async update(
    entity: SagaInstanceEntity,
    transitions: {
      transitionType: SagaTransitionType;
      fromState: string;
      toState: string;
      eventId: string;
      causationId: string;
      eventType: string;
    }[] = []
  ): Promise<SagaInstanceEntity> {
    return this.prisma.client.$transaction(async (tx) => {
      // 1. Fetch current version to verify optimistic lock
      const current = await tx.sagaInstance.findUnique({
        where: { id: entity.id },
      });
      if (!current) {
        throw new Error(`SagaInstance with id ${entity.id} was not found`);
      }
      if (current.version !== entity.version) {
        throw new ConflictException(
          `Optimistic locking failure: SagaInstance with id ${entity.id} and version ${entity.version} was updated concurrently.`
        );
      }

      // 2. Perform the update
      const updatedModel = await tx.sagaInstance.update({
        where: { id: entity.id },
        data: {
          status: entity.status,
          orderValidationStatus: entity.orderValidationStatus,
          completedAt: entity.completedAt,
          version: { increment: 1 },
          failureReason: entity.failureReason,
          failedAt: entity.failedAt,
          originService: entity.originService,
          stateUpdatedAt: entity.stateUpdatedAt,
          retryCount: entity.retryCount,
          lastRetryAt: entity.lastRetryAt,
          nextRetryAt: entity.nextRetryAt,
          currentCommandId: entity.currentCommandId,
          retryHandoffAt: entity.retryHandoffAt,
        },
      });

      // 3. Write transition audit records passed in
      for (const t of transitions) {
        await tx.sagaTransition.create({
          data: {
            sagaId: entity.id,
            correlationId: entity.correlationId,
            transitionType: t.transitionType,
            fromState: t.fromState,
            toState: t.toState,
            eventId: t.eventId,
            causationId: t.causationId,
            eventType: t.eventType,
          },
        });
      }

      return this.mapToEntity(updatedModel);
    });
  }

  /**
   * Discovers all incomplete saga instances (status !== CLOSED) for recovery.
   * Excludes sagas where order validation was rejected (terminal failure, forward path blocked).
   */
  async findRecoverableSagas(): Promise<SagaInstanceEntity[]> {
    const models = await this.prisma.client.sagaInstance.findMany({
      where: {
        status: {
          not: SagaStatus.CLOSED,
        },
        orderValidationStatus: {
          not: OrderValidationStatus.REJECTED,
        },
        failureReason: null, // Exclude failed ones too
      },
      orderBy: {
        createdAt: 'asc',
      },
    });
    return models.map((model) => this.mapToEntity(model));
  }

  /**
   * Find stalled Sagas that are eligible for timeout retry.
   * Implements strict predicates separating initial timeout from retry scheduler timings.
   */
  async findStalledSagas(
    now: Date,
    batchSize: number,
    stepTimeoutMs: number,
    handoffTimeoutMs: number
  ): Promise<SagaInstanceEntity[]> {
    const initialCutoff = new Date(now.getTime() - stepTimeoutMs);
    const retryCutoff = new Date(now.getTime() - stepTimeoutMs);
    const handoffCutoff = new Date(now.getTime() - handoffTimeoutMs);

    const models = await this.prisma.client.sagaInstance.findMany({
      where: {
        status: {
          in: [
            SagaStatus.LEDGER_PENDING,
            SagaStatus.ELIGIBILITY_PENDING,
            SagaStatus.BALANCE_PENDING,
          ],
        },
        orderValidationStatus: {
          not: OrderValidationStatus.REJECTED,
        },
        failureReason: null,
        OR: [
          {
            retryHandoffAt: null,
            nextRetryAt: null,
            retryCount: 0,
            stateUpdatedAt: {
              lte: initialCutoff,
            },
          },
          {
            retryHandoffAt: null,
            nextRetryAt: {
              not: null,
              lte: retryCutoff,
            },
          },
          {
            retryHandoffAt: {
              not: null,
              lte: handoffCutoff,
            },
          },
        ],
      },
      take: batchSize,
      orderBy: {
        stateUpdatedAt: 'asc',
      },
    });

    return models.map((model) => this.mapToEntity(model));
  }
}
