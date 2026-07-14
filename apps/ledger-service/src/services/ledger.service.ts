import * as crypto from 'crypto';

import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import {
  LEDGER_ENTRY_RECORDED,
  LEDGER_RECORDING_FAILED,
  RecordLedgerEntryCommand,
  RecordLedgerEntryPayload,
} from '@surgepay/events';

import { LedgerEntryEntity } from '../entities/ledger-entry.entity';
import { OutboxEventEntity } from '../entities/outbox-event.entity';
import { PrismaService } from '../prisma/prisma.service';
import { LedgerRepository } from '../repositories/ledger.repository';
import { OutboxRepository } from '../repositories/outbox.repository';

@Injectable()
export class LedgerService {
  constructor(
    private readonly ledgerRepository: LedgerRepository,
    private readonly outboxRepository: OutboxRepository,
    private readonly prisma: PrismaService,
    private readonly logger: LoggerService
  ) {
    this.logger.setContext('LedgerService');
  }

  async recordEntry(
    payload: RecordLedgerEntryPayload,
    commandEnvelope: RecordLedgerEntryCommand
  ): Promise<{ success: boolean; entry?: LedgerEntryEntity; reason?: string }> {
    try {
      return await this.prisma.client.$transaction(async (tx) => {
        // Business payload validation
        if (
          !payload ||
          typeof payload.amount !== 'number' ||
          payload.amount <= 0 ||
          !payload.currency ||
          !payload.merchantId ||
          !payload.paymentId ||
          !payload.entryType
        ) {
          const reason = `Invalid ledger entry payload: amount must be > 0 and merchantId, paymentId, currency, and entryType are required.`;
          this.logger.error(reason, { payload });

          const outbox = OutboxEventEntity.create({
            aggregateId: crypto.randomUUID(),
            aggregateType: 'LedgerEntry',
            eventType: LEDGER_RECORDING_FAILED,
            payload: {
              paymentId: payload?.paymentId || '',
              merchantId: payload?.merchantId || '',
              amount: payload?.amount || 0,
              currency: payload?.currency || '',
              reason,
              failedAt: new Date().toISOString(),
            },
            requestId: commandEnvelope.requestId || '',
            correlationId: commandEnvelope.correlationId || '',
            causationId: commandEnvelope.eventId || '',
          });

          await this.outboxRepository.save(outbox, tx);
          return { success: false, reason };
        }

        const entry = LedgerEntryEntity.create({
          paymentId: payload.paymentId,
          merchantId: payload.merchantId,
          amount: payload.amount,
          currency: payload.currency,
          entryType: payload.entryType,
          description: payload.description || '',
          sourceCommandId: commandEnvelope.eventId,
          correlationId: commandEnvelope.correlationId,
          causationId: commandEnvelope.eventId,
          sagaId: commandEnvelope.correlationId,
        });

        const createdEntry = await this.ledgerRepository.create(entry, tx);

        const outbox = OutboxEventEntity.create({
          aggregateId: createdEntry.id,
          aggregateType: 'LedgerEntry',
          eventType: LEDGER_ENTRY_RECORDED,
          payload: {
            entryId: createdEntry.id,
            paymentId: createdEntry.paymentId,
            merchantId: createdEntry.merchantId,
            amount: createdEntry.amount,
            currency: createdEntry.currency,
            recordedAt: createdEntry.createdAt.toISOString(),
          },
          requestId: commandEnvelope.requestId || '',
          correlationId: commandEnvelope.correlationId || '',
          causationId: commandEnvelope.eventId || '',
        });

        await this.outboxRepository.save(outbox, tx);

        return { success: true, entry: createdEntry };
      });
    } catch (err: unknown) {
      const prismaError = err as { code?: string };
      if (prismaError.code === 'P2002') {
        // Unique constraint collision on sourceCommandId
        const existing = await this.ledgerRepository.findBySourceCommandId(commandEnvelope.eventId);
        if (existing) {
          // Verify that financial/business fields match
          const matches =
            existing.paymentId === payload.paymentId &&
            existing.merchantId === payload.merchantId &&
            existing.amount === payload.amount &&
            existing.currency === payload.currency &&
            existing.entryType === payload.entryType;

          if (matches) {
            this.logger.warn('Idempotency collision hit: record already exists and payload matches. Treating as success.', {
              sourceCommandId: commandEnvelope.eventId,
            });
            return { success: true, entry: existing };
          } else {
            const reason = 'Idempotency collision hit but business/financial payload does not match existing entry.';
            this.logger.error(reason, {
              sourceCommandId: commandEnvelope.eventId,
              existing,
              payload,
            });

            // Write LedgerRecordingFailed to outbox in a separate transaction
            await this.prisma.client.$transaction(async (tx) => {
              const outbox = OutboxEventEntity.create({
                aggregateId: crypto.randomUUID(),
                aggregateType: 'LedgerEntry',
                eventType: LEDGER_RECORDING_FAILED,
                payload: {
                  paymentId: payload.paymentId || '',
                  merchantId: payload.merchantId || '',
                  amount: payload.amount || 0,
                  currency: payload.currency || '',
                  reason,
                  failedAt: new Date().toISOString(),
                },
                requestId: commandEnvelope.requestId || '',
                correlationId: commandEnvelope.correlationId || '',
                causationId: commandEnvelope.eventId || '',
              });
              await this.outboxRepository.save(outbox, tx);
            });

            return { success: false, reason };
          }
        }
      }
      throw err;
    }
  }
}
