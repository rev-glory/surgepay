import { ConflictException } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';

import {
  OrderValidationStatus,
  SagaStatus,
  SagaTransitionType,
} from '../../generated/client';
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
        update: jest.Mock;
        updateMany: jest.Mock;
        findMany: jest.Mock;
      };
      sagaTransition: {
        create: jest.Mock;
      };
      $transaction: jest.Mock;
    };
  };

  beforeEach(async () => {
    prismaMock = {
      client: {
        sagaInstance: {
          create: jest.fn(),
          findUnique: jest.fn(),
          update: jest.fn(),
          updateMany: jest.fn(),
          findMany: jest.fn(),
        },
        sagaTransition: {
          create: jest.fn(),
        },
        $transaction: jest.fn().mockImplementation((cb) => cb(prismaMock.client)),
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
  const merchantId = 'merch_xyz';
  const amount = 5000;
  const currency = 'USD';

  describe('create', () => {
    it('should invoke prisma.create with matching properties and return the entity', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId, merchantId, amount, currency });
      const dbMockResponse = {
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
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
        failureReason: entity.failureReason,
        failedAt: entity.failedAt,
        originService: entity.originService,
      };
      prismaMock.client.sagaInstance.create.mockResolvedValue(dbMockResponse);

      const result = await repository.create(entity);

      expect(prismaMock.client.sagaInstance.create).toHaveBeenCalledWith({
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
          recoveredAt: entity.recoveredAt,
          recoveryCount: entity.recoveryCount,
          recoveryReason: entity.recoveryReason,
        },
      });
      expect(result.id).toBe(entity.id);
      expect(result.correlationId).toBe(entity.correlationId);
      expect(result.merchantId).toBe(entity.merchantId);
      expect(result.amount).toBe(entity.amount);
      expect(result.currency).toBe(entity.currency);
    });
  });

  describe('findById', () => {
    it('should query findUnique by id and return mapped entity', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId, merchantId, amount, currency });
      const dbMockResponse = {
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
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
        failureReason: entity.failureReason,
        failedAt: entity.failedAt,
        originService: entity.originService,
      };
      prismaMock.client.sagaInstance.findUnique.mockResolvedValue(dbMockResponse);

      const result = await repository.findById(entity.id);

      expect(prismaMock.client.sagaInstance.findUnique).toHaveBeenCalledWith({
        where: { id: entity.id },
      });
      expect(result).toBeInstanceOf(SagaInstanceEntity);
      expect(result!.id).toBe(entity.id);
      expect(result!.merchantId).toBe(entity.merchantId);
      expect(result!.amount).toBe(entity.amount);
      expect(result!.currency).toBe(entity.currency);
    });
  });

  describe('findByPaymentId', () => {
    it('should query findUnique by paymentId and return mapped entity', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId, merchantId, amount, currency });
      const dbMockResponse = {
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
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
        failureReason: entity.failureReason,
        failedAt: entity.failedAt,
        originService: entity.originService,
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
      const entity = SagaInstanceEntity.create({ paymentId, correlationId, merchantId, amount, currency });
      const dbMockResponse = {
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
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
        failureReason: entity.failureReason,
        failedAt: entity.failedAt,
        originService: entity.originService,
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
    it('should query current version, verify locking, call update, and commit transitions', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId, merchantId, amount, currency });
      entity.confirmOrder();
      entity.transitionTo(SagaStatus.LEDGER_RECORDED);

      const dbCurrentResponse = {
        id: entity.id,
        paymentId: entity.paymentId,
        correlationId: entity.correlationId,
        status: SagaStatus.LEDGER_PENDING,
        orderValidationStatus: OrderValidationStatus.PENDING,
        merchantId: entity.merchantId,
        amount: entity.amount,
        currency: entity.currency,
        version: 0,
        startedAt: entity.startedAt,
        completedAt: null,
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
        failureReason: null,
        failedAt: null,
        originService: null,
      };
      prismaMock.client.sagaInstance.findUnique.mockResolvedValueOnce(dbCurrentResponse);

      const dbMockResponse = {
        id: entity.id,
        paymentId: entity.paymentId,
        correlationId: entity.correlationId,
        status: SagaStatus.LEDGER_RECORDED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
        merchantId: entity.merchantId,
        amount: entity.amount,
        currency: entity.currency,
        version: 1, // incremented
        startedAt: entity.startedAt,
        completedAt: entity.completedAt,
        createdAt: entity.createdAt,
        updatedAt: new Date(),
        failureReason: null,
        failedAt: null,
        originService: null,
      };
      prismaMock.client.sagaInstance.update.mockResolvedValue(dbMockResponse);

      const result = await repository.update(entity, [
        {
          transitionType: SagaTransitionType.ORDER_VALIDATION,
          fromState: OrderValidationStatus.PENDING,
          toState: OrderValidationStatus.CONFIRMED,
          eventId: 'evt_1',
          causationId: 'cause_1',
          eventType: 'OrderEligibilityConfirmed',
        },
      ]);

      expect(prismaMock.client.sagaInstance.findUnique).toHaveBeenCalledWith({
        where: { id: entity.id },
      });
      expect(prismaMock.client.sagaInstance.update).toHaveBeenCalledWith({
        where: { id: entity.id },
        data: {
          status: SagaStatus.LEDGER_RECORDED,
          orderValidationStatus: OrderValidationStatus.CONFIRMED,
          completedAt: entity.completedAt,
          version: { increment: 1 },
          failureReason: null,
          failedAt: null,
          originService: null,
          stateUpdatedAt: entity.stateUpdatedAt,
          retryCount: entity.retryCount,
          lastRetryAt: entity.lastRetryAt,
          nextRetryAt: entity.nextRetryAt,
          currentCommandId: entity.currentCommandId,
          retryHandoffAt: entity.retryHandoffAt,
          recoveredAt: entity.recoveredAt,
          recoveryCount: entity.recoveryCount,
          recoveryReason: entity.recoveryReason,
        },
      });
      expect(prismaMock.client.sagaTransition.create).toHaveBeenCalledWith({
        data: {
          sagaId: entity.id,
          correlationId: entity.correlationId,
          transitionType: SagaTransitionType.ORDER_VALIDATION,
          fromState: OrderValidationStatus.PENDING,
          toState: OrderValidationStatus.CONFIRMED,
          eventId: 'evt_1',
          causationId: 'cause_1',
          eventType: 'OrderEligibilityConfirmed',
        },
      });
      expect(result.status).toBe(SagaStatus.LEDGER_RECORDED);
      expect(result.version).toBe(1);
    });

    it('should throw ConflictException if database version does not match entity version', async () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId, merchantId, amount, currency });
      
      const dbCurrentResponse = {
        id: entity.id,
        paymentId: entity.paymentId,
        correlationId: entity.correlationId,
        status: SagaStatus.LEDGER_PENDING,
        orderValidationStatus: OrderValidationStatus.PENDING,
        merchantId: entity.merchantId,
        amount: entity.amount,
        currency: entity.currency,
        version: 5, // mismatch
        startedAt: entity.startedAt,
        completedAt: null,
        createdAt: entity.createdAt,
        updatedAt: entity.updatedAt,
        failureReason: null,
        failedAt: null,
        originService: null,
      };
      prismaMock.client.sagaInstance.findUnique.mockResolvedValueOnce(dbCurrentResponse);

      await expect(repository.update(entity)).rejects.toThrow(ConflictException);
    });
  });

  describe('findRecoverableSagas', () => {
    it('should query findMany for all records where status is not CLOSED and orderValidationStatus is not REJECTED', async () => {
      const mockDbRecords = [
        {
          id: 'saga_1',
          paymentId: 'pay_1',
          correlationId: 'saga_1',
          status: SagaStatus.LEDGER_PENDING,
          orderValidationStatus: OrderValidationStatus.PENDING,
          merchantId: 'merch_xyz',
          amount: 5000,
          currency: 'USD',
          version: 0,
          startedAt: new Date(),
          completedAt: null,
          createdAt: new Date(),
          updatedAt: new Date(),
          failureReason: null,
          failedAt: null,
          originService: null,
        },
        {
          id: 'saga_2',
          paymentId: 'pay_2',
          correlationId: 'saga_2',
          status: SagaStatus.BALANCE_RESERVED,
          orderValidationStatus: OrderValidationStatus.CONFIRMED,
          merchantId: 'merch_xyz',
          amount: 5000,
          currency: 'USD',
          version: 1,
          startedAt: new Date(),
          completedAt: null,
          createdAt: new Date(),
          updatedAt: new Date(),
          failureReason: null,
          failedAt: null,
          originService: null,
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
