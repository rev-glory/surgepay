import { ConflictException } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';

import { SagaStatus } from '../../generated/client';
import { PrismaService } from '../../prisma/prisma.service';
import { SagaInstanceEntity } from '../entities/saga-instance.entity';
import { SagaRepository } from './saga.repository';

describe('SagaRepository', () => {
  let repository: SagaRepository;
  let prismaMock: {
    client: {
      sagaInstance: {
        create: jest.Mock;
        findUnique: jest.Mock;
        updateMany: jest.Mock;
        findMany: jest.Mock;
      };
    };
  };

  beforeEach(async () => {
    prismaMock = {
      client: {
        sagaInstance: {
          create: jest.fn(),
          findUnique: jest.fn(),
          updateMany: jest.fn(),
          findMany: jest.fn(),
        },
      },
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        SagaRepository,
        {
          provide: PrismaService,
          useValue: prismaMock,
        },
      ],
    }).compile();

    repository = module.get<SagaRepository>(SagaRepository);
  });

  const correlationId = 'corr_998877';
  const paymentId = 'pay_554433';

  describe('create', () => {
    it('should invoke prisma.create with matching properties and return the entity', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });
      const dbMockResponse = {
        id: entity.id,
        paymentId: entity.paymentId,
        correlationId: entity.correlationId,
        status: entity.status,
        version: entity.version,
        startedAt: entity.startedAt,
        completedAt: entity.completedAt,
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
      };
      prismaMock.client.sagaInstance.create.mockResolvedValue(dbMockResponse);

      const result = await repository.create(entity);

      expect(prismaMock.client.sagaInstance.create).toHaveBeenCalledWith({
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
      expect(result.id).toBe(entity.id);
      expect(result.correlationId).toBe(entity.correlationId);
    });
  });

  describe('findById', () => {
    it('should query findUnique by id and return mapped entity', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });
      const dbMockResponse = {
        id: entity.id,
        paymentId: entity.paymentId,
        correlationId: entity.correlationId,
        status: entity.status,
        version: entity.version,
        startedAt: entity.startedAt,
        completedAt: entity.completedAt,
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
      };
      prismaMock.client.sagaInstance.findUnique.mockResolvedValue(dbMockResponse);

      const result = await repository.findById(entity.id);

      expect(prismaMock.client.sagaInstance.findUnique).toHaveBeenCalledWith({
        where: { id: entity.id },
      });
      expect(result).toBeInstanceOf(SagaInstanceEntity);
      expect(result!.id).toBe(entity.id);
    });

    it('should return null if saga is not found', async () => {
      prismaMock.client.sagaInstance.findUnique.mockResolvedValue(null);

      const result = await repository.findById('non-existent');

      expect(result).toBeNull();
    });
  });

  describe('findByPaymentId', () => {
    it('should query findUnique by paymentId and return mapped entity', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });
      const dbMockResponse = {
        id: entity.id,
        paymentId: entity.paymentId,
        correlationId: entity.correlationId,
        status: entity.status,
        version: entity.version,
        startedAt: entity.startedAt,
        completedAt: entity.completedAt,
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
      };
      prismaMock.client.sagaInstance.findUnique.mockResolvedValue(dbMockResponse);

      const result = await repository.findByPaymentId(paymentId);

      expect(prismaMock.client.sagaInstance.findUnique).toHaveBeenCalledWith({
        where: { paymentId },
      });
      expect(result).toBeInstanceOf(SagaInstanceEntity);
      expect(result!.paymentId).toBe(paymentId);
    });
  });

  describe('findByCorrelationId', () => {
    it('should query findUnique by correlationId and return mapped entity', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });
      const dbMockResponse = {
        id: entity.id,
        paymentId: entity.paymentId,
        correlationId: entity.correlationId,
        status: entity.status,
        version: entity.version,
        startedAt: entity.startedAt,
        completedAt: entity.completedAt,
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
      };
      prismaMock.client.sagaInstance.findUnique.mockResolvedValue(dbMockResponse);

      const result = await repository.findByCorrelationId(correlationId);

      expect(prismaMock.client.sagaInstance.findUnique).toHaveBeenCalledWith({
        where: { correlationId },
      });
      expect(result).toBeInstanceOf(SagaInstanceEntity);
      expect(result!.correlationId).toBe(correlationId);
    });
  });

  describe('update', () => {
    it('should call updateMany to filter by version and increment version upon success', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });
      entity.transitionTo(SagaStatus.LEDGER_RECORDED);

      prismaMock.client.sagaInstance.updateMany.mockResolvedValue({ count: 1 });

      const dbMockResponse = {
        id: entity.id,
        paymentId: entity.paymentId,
        correlationId: entity.correlationId,
        status: SagaStatus.LEDGER_RECORDED,
        version: 1, // incremented
        startedAt: entity.startedAt,
        completedAt: entity.completedAt,
        createdAt: entity.createdAt,
        updatedAt: new Date(),
      };
      prismaMock.client.sagaInstance.findUnique.mockResolvedValue(dbMockResponse);

      const result = await repository.update(entity);

      expect(prismaMock.client.sagaInstance.updateMany).toHaveBeenCalledWith({
        where: {
          id: entity.id,
          version: entity.version,
        },
        data: {
          status: SagaStatus.LEDGER_RECORDED,
          completedAt: entity.completedAt,
          version: { increment: 1 },
        },
      });
      expect(result.status).toBe(SagaStatus.LEDGER_RECORDED);
      expect(result.version).toBe(1);
    });

    it('should throw ConflictException if updateMany returns count 0 indicating a concurrent update', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });
      prismaMock.client.sagaInstance.updateMany.mockResolvedValue({ count: 0 });

      await expect(repository.update(entity)).rejects.toThrow(ConflictException);
    });
  });

  describe('findRecoverableSagas', () => {
    it('should query findMany for all records where status is not CLOSED', async () => {
      const mockDbRecords = [
        {
          id: 'saga_1',
          paymentId: 'pay_1',
          correlationId: 'saga_1',
          status: SagaStatus.LEDGER_PENDING,
          version: 0,
          startedAt: new Date(),
          completedAt: null,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
        {
          id: 'saga_2',
          paymentId: 'pay_2',
          correlationId: 'saga_2',
          status: SagaStatus.BALANCE_RESERVED,
          version: 1,
          startedAt: new Date(),
          completedAt: null,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      ];

      prismaMock.client.sagaInstance.findMany.mockResolvedValue(mockDbRecords);

      const results = await repository.findRecoverableSagas();

      expect(prismaMock.client.sagaInstance.findMany).toHaveBeenCalledWith({
        where: {
          status: {
            not: SagaStatus.CLOSED,
          },
        },
        orderBy: {
          createdAt: 'asc',
        },
      });
      expect(results.length).toBe(2);
      expect(results[0]!.status).toBe(SagaStatus.LEDGER_PENDING);
      expect(results[1]!.status).toBe(SagaStatus.BALANCE_RESERVED);
    });
  });
});
