import * as crypto from 'crypto';

import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import {
  BALANCE_RESERVATION_FAILED,
  BALANCE_RESERVED,
  BALANCE_REVERSED,
  ReserveBalanceCommand,
  ReserveBalancePayload,
  ReverseBalanceCommand,
  ReverseBalancePayload,
} from '@surgepay/events';

import { OutboxEventEntity } from '../entities/outbox-event.entity';
import { PrismaService } from '../prisma/prisma.service';
import { BalanceRepository } from '../repositories/balance.repository';
import { OutboxRepository } from '../repositories/outbox.repository';

interface CommandWithRequestId {
  requestId?: string;
}

@Injectable()
export class BalanceService {
  constructor(
    private readonly balanceRepository: BalanceRepository,
    private readonly outboxRepository: OutboxRepository,
    private readonly prisma: PrismaService,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('BalanceService');
  }

  async reserve(
    payload: ReserveBalancePayload,
    commandEnvelope: ReserveBalanceCommand
  ): Promise<{ success: boolean; reason?: string }> {
    try {
      return await this.prisma.client.$transaction(async (tx) => {
        const logContext = {
          commandId: commandEnvelope.eventId,
          paymentId: payload?.paymentId,
          merchantId: payload?.merchantId,
          amount: payload?.amount,
          currency: payload?.currency,
          correlationId: commandEnvelope.correlationId,
          sagaId: commandEnvelope.sagaId,
        };

        // 1. Validate payload structure
        if (
          !payload ||
          typeof payload.amount !== 'number' ||
          payload.amount <= 0 ||
          !payload.currency ||
          !payload.merchantId ||
          !payload.paymentId
        ) {
          const reason = 'Invalid balance reservation payload: amount must be > 0 and merchantId, paymentId, and currency are required.';
          this.logger.error(reason, { payload });

          const outbox = OutboxEventEntity.create({
            aggregateId: crypto.randomUUID(),
            aggregateType: 'MerchantBalance',
            eventType: BALANCE_RESERVATION_FAILED,
            payload: {
              paymentId: payload?.paymentId || '',
              merchantId: payload?.merchantId || '',
              amount: payload?.amount || 0,
              currency: payload?.currency || '',
              reason,
              failedAt: new Date().toISOString(),
            },
            requestId: (commandEnvelope as CommandWithRequestId).requestId || '',
            correlationId: commandEnvelope.correlationId || '',
            causationId: commandEnvelope.eventId || '',
          });

          await this.outboxRepository.save(outbox, tx);
          return { success: false, reason };
        }

        // 2. Lookup Strategy to distinguish BALANCE_NOT_FOUND from CURRENCY_MISMATCH
        const merchantBalances = await this.balanceRepository.findByMerchantId(payload.merchantId, tx);

        if (merchantBalances.length === 0) {
          const reason = `Merchant balance projection not found for merchant ${payload.merchantId}`;
          this.logger.warn(reason, logContext);

          const outbox = OutboxEventEntity.create({
            aggregateId: crypto.randomUUID(),
            aggregateType: 'MerchantBalance',
            eventType: BALANCE_RESERVATION_FAILED,
            payload: {
              paymentId: payload.paymentId,
              merchantId: payload.merchantId,
              amount: payload.amount,
              currency: payload.currency,
              reason,
              failedAt: new Date().toISOString(),
            },
            requestId: (commandEnvelope as CommandWithRequestId).requestId || '',
            correlationId: commandEnvelope.correlationId || '',
            causationId: commandEnvelope.eventId || '',
          });

          await this.outboxRepository.save(outbox, tx);
          return { success: false, reason };
        }

        const matchingBalance = merchantBalances.find(
          (b) => b.currency.toUpperCase() === payload.currency.toUpperCase()
        );

        if (!matchingBalance) {
          const reason = `Currency mismatch: merchant ${payload.merchantId} does not support currency ${payload.currency}`;
          this.logger.warn(reason, logContext);

          const outbox = OutboxEventEntity.create({
            aggregateId: crypto.randomUUID(),
            aggregateType: 'MerchantBalance',
            eventType: BALANCE_RESERVATION_FAILED,
            payload: {
              paymentId: payload.paymentId,
              merchantId: payload.merchantId,
              amount: payload.amount,
              currency: payload.currency,
              reason,
              failedAt: new Date().toISOString(),
            },
            requestId: (commandEnvelope as CommandWithRequestId).requestId || '',
            correlationId: commandEnvelope.correlationId || '',
            causationId: commandEnvelope.eventId || '',
          });

          await this.outboxRepository.save(outbox, tx);
          return { success: false, reason };
        }

        // 3. Perform atomic reservation
        const reserved = await this.balanceRepository.reserveFunds(
          payload.merchantId,
          payload.currency,
          payload.amount,
          tx
        );

        if (!reserved) {
          const reason = 'Insufficient available balance';
          this.logger.warn(reason, logContext);

          const outbox = OutboxEventEntity.create({
            aggregateId: matchingBalance.id,
            aggregateType: 'MerchantBalance',
            eventType: BALANCE_RESERVATION_FAILED,
            payload: {
              paymentId: payload.paymentId,
              merchantId: payload.merchantId,
              amount: payload.amount,
              currency: payload.currency,
              reason,
              failedAt: new Date().toISOString(),
            },
            requestId: (commandEnvelope as CommandWithRequestId).requestId || '',
            correlationId: commandEnvelope.correlationId || '',
            causationId: commandEnvelope.eventId || '',
          });

          await this.outboxRepository.save(outbox, tx);
          return { success: false, reason };
        }

        // Success path
        const reservationId = crypto.randomUUID();
        const outbox = OutboxEventEntity.create({
          aggregateId: matchingBalance.id,
          aggregateType: 'MerchantBalance',
          eventType: BALANCE_RESERVED,
          payload: {
            reservationId,
            paymentId: payload.paymentId,
            merchantId: payload.merchantId,
            amount: payload.amount,
            currency: payload.currency,
            reservedAt: new Date().toISOString(),
          },
          requestId: (commandEnvelope as CommandWithRequestId).requestId || '',
          correlationId: commandEnvelope.correlationId || '',
          causationId: commandEnvelope.eventId || '',
        });

        await this.outboxRepository.save(outbox, tx);
        return { success: true };
      });
    } catch (err: unknown) {
      this.logger.error('Database error during balance reservation transaction', err as Error);
      throw err;
    }
  }

  /**
   * Releases a previously reserved amount for a payment.
   * This is the balance compensation operation for doc-v3 Section 6.2 Scenario 3.
   *
   * Idempotency is enforced at two levels:
   *   1. Business-layer check: findReversalByPaymentId before releaseFunds.
   *   2. DB-level enforcement: BalanceReversal.paymentId @unique fires atomically
   *      if a concurrent reversal command commits first.
   *
   * All three operations (findReversalByPaymentId, createReversal, releaseFunds, outbox write)
   * execute inside a single Prisma transaction, so either all succeed or none do.
   */
  async reverse(
    payload: ReverseBalancePayload,
    commandEnvelope: ReverseBalanceCommand
  ): Promise<{ success: boolean; reason?: string }> {
    const logContext = {
      commandId: commandEnvelope.eventId,
      paymentId: payload.paymentId,
      merchantId: payload.merchantId,
      correlationId: commandEnvelope.correlationId,
      sagaId: commandEnvelope.sagaId,
    };

    try {
      return await this.prisma.client.$transaction(async (tx) => {
        // 1. Validate payload
        if (
          !payload ||
          typeof payload.amount !== 'number' ||
          payload.amount <= 0 ||
          !payload.currency ||
          !payload.merchantId ||
          !payload.paymentId ||
          !payload.reason
        ) {
          const reason = 'Invalid balance reversal payload: amount, merchantId, paymentId, currency, and reason are required.';
          this.logger.error(reason, logContext);
          return { success: false, reason };
        }

        // 2. Business-level idempotency check: has this payment already been reversed?
        const existingReversal = await this.balanceRepository.findReversalByPaymentId(
          payload.paymentId,
          tx
        );
        if (existingReversal) {
          this.logger.warn('Balance reversal already exists for this payment. Idempotent skip.', {
            ...logContext,
            existingReversalId: existingReversal.id,
          });
          return { success: true };
        }

        // 3. Verify merchant balance exists and supports the currency
        const merchantBalances = await this.balanceRepository.findByMerchantId(payload.merchantId, tx);
        const matchingBalance = merchantBalances.find(
          (b) => b.currency.toUpperCase() === payload.currency.toUpperCase()
        );

        if (!matchingBalance) {
          const reason = `Merchant balance not found for merchant ${payload.merchantId} and currency ${payload.currency}`;
          this.logger.error(reason, logContext);
          return { success: false, reason };
        }

        // 4. Create durable reversal audit record.
        // The paymentId @unique constraint fires atomically if two commands race.
        await this.balanceRepository.createReversal(
          {
            paymentId: payload.paymentId,
            merchantId: payload.merchantId,
            currency: payload.currency,
            amount: payload.amount,
            commandId: commandEnvelope.eventId,
          },
          tx
        );

        // 5. Atomic conditional release: reserved -= amount, available += amount
        const released = await this.balanceRepository.releaseFunds(
          payload.merchantId,
          payload.currency,
          payload.amount,
          tx
        );

        if (!released) {
          // reserved < amount — guard prevents negative reserved values.
          // This should not occur in a healthy saga (the forward reservation succeeded),
          // but is handled defensively.
          const reason = `Insufficient reserved balance for reversal: merchant=${payload.merchantId}, currency=${payload.currency}, amount=${payload.amount}`;
          this.logger.error(reason, logContext);
          return { success: false, reason };
        }

        // 6. Write BalanceReversed to the outbox inside the same transaction
        const reversalId = crypto.randomUUID();
        const outbox = OutboxEventEntity.create({
          aggregateId: matchingBalance.id,
          aggregateType: 'MerchantBalance',
          eventType: BALANCE_REVERSED,
          payload: {
            reversalId,
            paymentId: payload.paymentId,
            merchantId: payload.merchantId,
            amount: payload.amount,
            currency: payload.currency,
            reversedAt: new Date().toISOString(),
          },
          requestId: (commandEnvelope as CommandWithRequestId).requestId || '',
          correlationId: commandEnvelope.correlationId || '',
          causationId: commandEnvelope.eventId || '',
        });

        await this.outboxRepository.save(outbox, tx);

        this.logger.info('Balance reversal committed successfully', {
          ...logContext,
          reversalId,
        });

        return { success: true };
      });
    } catch (err: unknown) {
      const prismaError = err as { code?: string };
      if (prismaError.code === 'P2002') {
        // paymentId unique constraint fired — concurrent reversal already committed.
        // Treat as idempotent success.
        this.logger.warn(
          'Concurrent balance reversal race: unique constraint fired. Treating as idempotent success.',
          logContext
        );
        return { success: true };
      }
      this.logger.error('Database error during balance reversal transaction', err as Error, logContext);
      throw err;
    }
  }
}
