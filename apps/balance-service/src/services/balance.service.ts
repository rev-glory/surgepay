import * as crypto from 'crypto';

import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import {
  BALANCE_RESERVATION_FAILED,
  BALANCE_RESERVED,
  ReserveBalanceCommand,
  ReserveBalancePayload,
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
}
