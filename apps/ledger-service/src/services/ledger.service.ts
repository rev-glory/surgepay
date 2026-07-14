import * as crypto from 'crypto';

import { Injectable } from '@nestjs/common';

import { LoggerService } from '@surgepay/common';
import {
  LEDGER_ENTRY_RECORDED,
  LEDGER_RECORDING_FAILED,
  LEDGER_REVERSED,
  RecordLedgerEntryCommand,
  RecordLedgerEntryPayload,
  ReverseLedgerEntryCommand,
  ReverseLedgerEntryPayload,
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
            requestId: '',
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
          requestId: '',
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
                requestId: '',
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

  /**
   * Appends an offsetting CREDIT entry for the original DEBIT associated with the payment.
   * This is the ledger compensation operation for doc-v3 Section 6.2 Scenarios 1, 2, and 3.
   *
   * Idempotency is enforced at two levels:
   *   1. Business-layer check: findCompensationByOriginalEntryId inside the transaction.
   *   2. DB-level enforcement: partial unique index on reversalOf (WHERE "reversalOf" IS NOT NULL)
   *      prevents concurrent duplicate compensation entries from both committing.
   *
   * The ledger is strictly append-only. No UPDATE or DELETE paths exist in this repository layer.
   * The original DEBIT entry is never modified.
   */
  async reverseEntry(
    payload: ReverseLedgerEntryPayload,
    commandEnvelope: ReverseLedgerEntryCommand
  ): Promise<{ success: boolean; reversalEntry?: LedgerEntryEntity; reason?: string }> {
    const logContext = {
      commandId: commandEnvelope.eventId,
      paymentId: payload.paymentId,
      merchantId: payload.merchantId,
      correlationId: commandEnvelope.correlationId,
      sagaId: commandEnvelope.sagaId,
    };

    try {
      return await this.prisma.client.$transaction(async (tx) => {
        // 1. Find the original DEBIT entry for this payment
        const originalEntry = await this.ledgerRepository.findOriginalByPaymentId(payload.paymentId, tx);
        if (!originalEntry) {
          const reason = `No original ledger entry found for paymentId ${payload.paymentId}. Cannot compensate.`;
          this.logger.error(reason, logContext);
          // Not a transient error — the original entry must exist. Return a failed result
          // without throwing, so the Inbox marks PROCESSED and the offset is committed.
          return { success: false, reason };
        }

        // 2. Idempotency check: has a compensation entry already been created?
        const existingCompensation = await this.ledgerRepository.findCompensationByOriginalEntryId(
          originalEntry.id,
          tx
        );
        if (existingCompensation) {
          this.logger.warn('Ledger reversal already exists for this original entry. Idempotent skip.', {
            ...logContext,
            originalEntryId: originalEntry.id,
            existingReversalId: existingCompensation.id,
          });
          return { success: true, reversalEntry: existingCompensation };
        }

        // 3. Append the compensation CREDIT entry.
        // reversalOf links this entry to the original for audit and idempotency enforcement.
        const reversalEntry = LedgerEntryEntity.create({
          paymentId: payload.paymentId,
          merchantId: payload.merchantId,
          amount: originalEntry.amount,
          currency: originalEntry.currency,
          entryType: 'CREDIT',
          description: `Compensation reversal for payment ${payload.paymentId}. Reason: ${payload.reason}`,
          sourceCommandId: commandEnvelope.eventId,
          correlationId: commandEnvelope.correlationId,
          causationId: commandEnvelope.eventId,
          sagaId: commandEnvelope.sagaId || commandEnvelope.correlationId,
          reversalOf: originalEntry.id,
        });

        const createdReversal = await this.ledgerRepository.create(reversalEntry, tx);

        // 4. Persist LedgerReversed to the outbox inside the same transaction
        const outbox = OutboxEventEntity.create({
          aggregateId: createdReversal.id,
          aggregateType: 'LedgerEntry',
          eventType: LEDGER_REVERSED,
          payload: {
            reversalEntryId: createdReversal.id,
            originalEntryId: originalEntry.id,
            paymentId: createdReversal.paymentId,
            merchantId: createdReversal.merchantId,
            amount: createdReversal.amount,
            currency: createdReversal.currency,
            reversedAt: createdReversal.createdAt.toISOString(),
          },
          requestId: '',
          correlationId: commandEnvelope.correlationId || '',
          causationId: commandEnvelope.eventId || '',
        });

        await this.outboxRepository.save(outbox, tx);

        this.logger.info('Ledger reversal entry appended successfully', {
          ...logContext,
          originalEntryId: originalEntry.id,
          reversalEntryId: createdReversal.id,
        });

        return { success: true, reversalEntry: createdReversal };
      });
    } catch (err: unknown) {
      const prismaError = err as { code?: string };
      if (prismaError.code === 'P2002') {
        // The partial unique index fired — a concurrent reversal already committed.
        // Find and return the existing entry as an idempotent success.
        const originalEntry = await this.ledgerRepository.findOriginalByPaymentId(payload.paymentId);
        if (originalEntry) {
          const existingCompensation = await this.ledgerRepository.findCompensationByOriginalEntryId(
            originalEntry.id
          );
          if (existingCompensation) {
            this.logger.warn(
              'Concurrent ledger reversal race: unique constraint fired. Returning existing entry as idempotent success.',
              { ...logContext, existingReversalId: existingCompensation.id }
            );
            return { success: true, reversalEntry: existingCompensation };
          }
        }
      }
      this.logger.error('Database error during ledger reversal transaction', err as Error, logContext);
      throw err;
    }
  }
}
