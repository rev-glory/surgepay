import { Injectable, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';

import { LoggerService, TOPIC_REGISTRY } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import {
  CHECK_PAYOUT_ELIGIBILITY,
  RECORD_LEDGER_ENTRY,
  RESERVE_BALANCE,
  SCHEDULE_RETRY,
  type ScheduleRetryCommand,
} from '@surgepay/events';

import { SagaStatus } from '../../generated/client';
import { OrderOutboxEventEntity } from '../entities/order-outbox-event.entity';
import { OrderOutboxRepository } from '../repositories/order-outbox.repository';
import { SagaRepository } from '../repositories/saga.repository';

@Injectable()
export class SagaTimeoutScanner implements OnApplicationBootstrap, OnApplicationShutdown {
  private isRunning = false;
  private isScanning = false;
  private timeoutId?: NodeJS.Timeout;

  constructor(
    private readonly sagaRepository: SagaRepository,
    private readonly outboxRepository: OrderOutboxRepository,
    private readonly config: ConfigService,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('SagaTimeoutScanner');
  }

  async onApplicationBootstrap(): Promise<void> {
    this.logger.info('Saga Timeout Scanner starting...', {
      scanIntervalMs: this.config.saga.scanIntervalMs,
      stepTimeoutMs: this.config.saga.stepTimeoutMs,
      maxRetryAttempts: this.config.saga.maxRetryAttempts,
    });
    this.isRunning = true;
    this.scheduleNextScan();
  }

  async onApplicationShutdown(): Promise<void> {
    this.logger.info('Saga Timeout Scanner shutting down...');
    this.isRunning = false;
    if (this.timeoutId) {
      clearTimeout(this.timeoutId);
    }
  }

  private scheduleNextScan(): void {
    if (!this.isRunning) return;

    this.timeoutId = setTimeout(async () => {
      if (!this.isRunning) return;

      if (this.isScanning) {
        this.scheduleNextScan();
        return;
      }

      this.isScanning = true;
      try {
        await this.scanForTimeouts();
      } catch (err) {
        this.logger.error('Error during saga timeout scanning cycle', err as Error);
      } finally {
        this.isScanning = false;
        this.scheduleNextScan();
      }
    }, this.config.saga.scanIntervalMs);
  }

  private async scanForTimeouts(): Promise<void> {
    const now = new Date();
    const stalled = await this.sagaRepository.findStalledSagas(
      now,
      this.config.saga.batchSize,
      this.config.saga.stepTimeoutMs,
      this.config.saga.handoffTimeoutMs
    );

    if (stalled.length === 0) return;

    this.logger.info(`Discovered ${stalled.length} stalled sagas to recover`);

    for (const saga of stalled) {
      try {
        await this.processSagaTimeout(saga);
      } catch (err) {
        this.logger.error(`Failed to process timeout recovery for saga ${saga.id}`, err as Error);
      }
    }
  }

  private async processSagaTimeout(saga: any): Promise<void> {
    // 1. Resolve expected command type and payload
    let expectedCommandType = '';
    let payload: Record<string, any> = {};

    if (saga.status === SagaStatus.LEDGER_PENDING) {
      expectedCommandType = RECORD_LEDGER_ENTRY;
      payload = {
        paymentId: saga.paymentId,
        merchantId: saga.merchantId,
        amount: saga.amount,
        currency: saga.currency,
        entryType: 'DEBIT',
        description: `Payment ledger record for payment ${saga.paymentId}`,
      };
    } else if (saga.status === SagaStatus.ELIGIBILITY_PENDING) {
      expectedCommandType = CHECK_PAYOUT_ELIGIBILITY;
      payload = {
        paymentId: saga.paymentId,
        merchantId: saga.merchantId,
        amount: saga.amount,
        currency: saga.currency,
      };
    } else if (saga.status === SagaStatus.BALANCE_PENDING) {
      expectedCommandType = RESERVE_BALANCE;
      payload = {
        paymentId: saga.paymentId,
        merchantId: saga.merchantId,
        amount: saga.amount,
        currency: saga.currency,
      };
    } else {
      this.logger.warn(`Saga ${saga.id} in unsupported state for timeout scanning: ${saga.status}`);
      return;
    }

    const originalTopic = TOPIC_REGISTRY[expectedCommandType];
    if (!originalTopic) {
      this.logger.error(`No topic registered for command type: ${expectedCommandType}`);
      return;
    }

    const commandId = saga.currentCommandId || crypto.randomUUID();

    // 2. Build the original command envelope
    const originalEvent = {
      eventId: commandId,
      eventType: expectedCommandType,
      correlationId: saga.correlationId,
      causationId: saga.correlationId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload,
    };

    // 3. Build the ScheduleRetryCommand envelope
    const scheduleRetryCommand: ScheduleRetryCommand = {
      eventId: crypto.randomUUID(),
      eventType: SCHEDULE_RETRY,
      correlationId: saga.correlationId,
      causationId: saga.id,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        originalTopic,
        originalEvent,
        retryCount: saga.retryCount,
        maxAttempts: this.config.saga.maxRetryAttempts,
        baseDelayMs: this.config.saga.retryBaseDelayMs,
        maxDelayMs: this.config.saga.retryMaxDelayMs,
      },
    };

    // 4. Atomically persist handoff state and outbox event in one transaction
    this.logger.info(`Initiating retry handoff for stalled saga ${saga.id} in state ${saga.status}`, {
      sagaId: saga.id,
      currentCommandId: commandId,
      retryCount: saga.retryCount,
    });

    saga.startHandoff();

    const updatedSaga = await this.sagaRepository.update(saga, []);

    // Create and save transactional outbox event
    const outboxEvent = OrderOutboxEventEntity.create({
      eventType: SCHEDULE_RETRY,
      payload: scheduleRetryCommand,
    });

    // In local db boundary: order outbox is saved outside the transaction since
    // saga.repository.update handles the main status check, but wait, doing both
    // in sagaRepository.update transaction is even safer!
    // Let's modify sagaRepository to support passing outbox events or write it locally.
    // Since we want to commit both atomically, let's write to outbox in the same Prisma transaction!
    await (this.sagaRepository as any).prisma.client.$transaction(async (tx: any) => {
      // Re-read and check optimistic lock
      const current = await tx.sagaInstance.findUnique({
        where: { id: updatedSaga.id },
      });
      if (!current) {
        throw new Error(`SagaInstance with id ${updatedSaga.id} not found`);
      }
      if (current.version !== updatedSaga.version) {
        throw new Error('Optimistic lock check failed concurrently.');
      }

      // Update saga handoff
      await tx.sagaInstance.update({
        where: { id: updatedSaga.id },
        data: {
          retryHandoffAt: updatedSaga.retryHandoffAt,
          version: { increment: 1 },
        },
      });

      // Save to outbox
      await tx.orderOutboxEvent.create({
        data: {
          id: outboxEvent.id,
          eventType: outboxEvent.eventType,
          payload: outboxEvent.payload,
          status: outboxEvent.status,
          createdAt: outboxEvent.createdAt,
        },
      });
    });

    this.logger.info(`Saga timeout handoff successfully written to outbox`, {
      sagaId: saga.id,
      outboxId: outboxEvent.id,
    });
  }
}
