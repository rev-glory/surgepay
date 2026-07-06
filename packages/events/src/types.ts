import type * as constants from './constants';
import type { BaseEventEnvelope } from './envelope';

// --- PAYMENT LIFECYCLE PAYLOADS & EVENTS ---

export interface PaymentInitiatedPayload {
  paymentId: string;
  amount: number;
  currency: string;
  merchantId: string;
  orderId: string;
  paymentMethod: string;
  customerId?: string;
  metadata?: Record<string, unknown>;
}
export type PaymentInitiatedEvent = BaseEventEnvelope<PaymentInitiatedPayload> & {
  eventType: typeof constants.PAYMENT_INITIATED;
};

export interface PaymentCompletedPayload {
  paymentId: string;
  amount: number;
  currency: string;
  merchantId: string;
  orderId: string;
  processorTransactionId: string;
  completedAt: string;
}
export type PaymentCompletedEvent = BaseEventEnvelope<PaymentCompletedPayload> & {
  eventType: typeof constants.PAYMENT_COMPLETED;
};

export interface PaymentFailedPayload {
  paymentId: string;
  amount: number;
  currency: string;
  merchantId: string;
  orderId: string;
  errorCode: string;
  errorMessage: string;
  failedAt: string;
}
export type PaymentFailedEvent = BaseEventEnvelope<PaymentFailedPayload> & {
  eventType: typeof constants.PAYMENT_FAILED;
};

export interface PaymentRejectedPayload {
  paymentId: string;
  amount: number;
  currency: string;
  merchantId: string;
  orderId: string;
  reason: string;
  rejectedAt: string;
}
export type PaymentRejectedEvent = BaseEventEnvelope<PaymentRejectedPayload> & {
  eventType: typeof constants.PAYMENT_REJECTED;
};

export interface PaymentFlaggedPayload {
  paymentId: string;
  merchantId: string;
  score: number;
  reasons: string[];
  flaggedAt: string;
}
export type PaymentFlaggedEvent = BaseEventEnvelope<PaymentFlaggedPayload> & {
  eventType: typeof constants.PAYMENT_FLAGGED;
};

// --- SAGA COMMAND PAYLOADS & EVENTS ---

export interface RecordLedgerEntryPayload {
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  entryType: 'DEBIT' | 'CREDIT';
  description: string;
}
export type RecordLedgerEntryCommand = BaseEventEnvelope<RecordLedgerEntryPayload> & {
  eventType: typeof constants.RECORD_LEDGER_ENTRY;
};

export interface ReverseLedgerEntryPayload {
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  reason: string;
}
export type ReverseLedgerEntryCommand = BaseEventEnvelope<ReverseLedgerEntryPayload> & {
  eventType: typeof constants.REVERSE_LEDGER_ENTRY;
};

export interface CheckPayoutEligibilityPayload {
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
}
export type CheckPayoutEligibilityCommand = BaseEventEnvelope<CheckPayoutEligibilityPayload> & {
  eventType: typeof constants.CHECK_PAYOUT_ELIGIBILITY;
};

export interface ReserveBalancePayload {
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
}
export type ReserveBalanceCommand = BaseEventEnvelope<ReserveBalancePayload> & {
  eventType: typeof constants.RESERVE_BALANCE;
};

export interface ReverseBalancePayload {
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  reason: string;
}
export type ReverseBalanceCommand = BaseEventEnvelope<ReverseBalancePayload> & {
  eventType: typeof constants.REVERSE_BALANCE;
};

export interface NotifyMerchantPayload {
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  notificationType: 'PAYMENT_RECEIVED' | 'PAYMENT_FAILED' | 'REVERSED';
  destination: string;
}
export type NotifyMerchantCommand = BaseEventEnvelope<NotifyMerchantPayload> & {
  eventType: typeof constants.NOTIFY_MERCHANT;
};

// --- LEDGER SERVICE PAYLOADS & EVENTS ---

export interface LedgerEntryRecordedPayload {
  entryId: string;
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  recordedAt: string;
}
export type LedgerEntryRecordedEvent = BaseEventEnvelope<LedgerEntryRecordedPayload> & {
  eventType: typeof constants.LEDGER_ENTRY_RECORDED;
};

export interface LedgerRecordingFailedPayload {
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  reason: string;
  failedAt: string;
}
export type LedgerRecordingFailedEvent = BaseEventEnvelope<LedgerRecordingFailedPayload> & {
  eventType: typeof constants.LEDGER_RECORDING_FAILED;
};

export interface LedgerReversedPayload {
  reversalEntryId: string;
  originalEntryId: string;
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  reversedAt: string;
}
export type LedgerReversedEvent = BaseEventEnvelope<LedgerReversedPayload> & {
  eventType: typeof constants.LEDGER_REVERSED;
};

// --- RISK ENGINE PAYLOADS & EVENTS ---

export interface EligibilityApprovedPayload {
  paymentId: string;
  merchantId: string;
  approvedAt: string;
}
export type EligibilityApprovedEvent = BaseEventEnvelope<EligibilityApprovedPayload> & {
  eventType: typeof constants.ELIGIBILITY_APPROVED;
};

export interface EligibilityDeniedPayload {
  paymentId: string;
  merchantId: string;
  reason: string;
  deniedAt: string;
}
export type EligibilityDeniedEvent = BaseEventEnvelope<EligibilityDeniedPayload> & {
  eventType: typeof constants.ELIGIBILITY_DENIED;
};

// --- BALANCE SERVICE PAYLOADS & EVENTS ---

export interface BalanceReservedPayload {
  reservationId: string;
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  reservedAt: string;
}
export type BalanceReservedEvent = BaseEventEnvelope<BalanceReservedPayload> & {
  eventType: typeof constants.BALANCE_RESERVED;
};

export interface BalanceReservationFailedPayload {
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  reason: string;
  failedAt: string;
}
export type BalanceReservationFailedEvent = BaseEventEnvelope<BalanceReservationFailedPayload> & {
  eventType: typeof constants.BALANCE_RESERVATION_FAILED;
};

export interface BalanceReversedPayload {
  reversalId: string;
  paymentId: string;
  merchantId: string;
  amount: number;
  currency: string;
  reversedAt: string;
}
export type BalanceReversedEvent = BaseEventEnvelope<BalanceReversedPayload> & {
  eventType: typeof constants.BALANCE_REVERSED;
};

// --- NOTIFICATION/WEBHOOK SERVICE PAYLOADS & EVENTS ---

export interface MerchantNotifiedPayload {
  notificationId: string;
  paymentId: string;
  merchantId: string;
  notifiedAt: string;
}
export type MerchantNotifiedEvent = BaseEventEnvelope<MerchantNotifiedPayload> & {
  eventType: typeof constants.MERCHANT_NOTIFIED;
};

export interface NotificationDeferredPayload {
  notificationId: string;
  paymentId: string;
  merchantId: string;
  reason: string;
  deferredAt: string;
}
export type NotificationDeferredEvent = BaseEventEnvelope<NotificationDeferredPayload> & {
  eventType: typeof constants.NOTIFICATION_DEFERRED;
};

export interface NotificationDeliveryFailedPayload {
  notificationId: string;
  paymentId: string;
  merchantId: string;
  reason: string;
  failedAt: string;
}
export type NotificationDeliveryFailedEvent =
  BaseEventEnvelope<NotificationDeliveryFailedPayload> & {
    eventType: typeof constants.NOTIFICATION_DELIVERY_FAILED;
  };

// --- OPERATIONAL PAYLOADS & EVENTS ---

export interface RetryScheduledPayload {
  originalEventId: string;
  topic: string;
  attempt: number;
  maxAttempts: number;
  nextExecutionTime: string;
}
export type RetryScheduledEvent = BaseEventEnvelope<RetryScheduledPayload> & {
  eventType: typeof constants.RETRY_SCHEDULED;
};

export interface RetryExhaustedPayload {
  originalEventId: string;
  topic: string;
  attempts: number;
  exhaustedAt: string;
}
export type RetryExhaustedEvent = BaseEventEnvelope<RetryExhaustedPayload> & {
  eventType: typeof constants.RETRY_EXHAUSTED;
};

export interface MessageMovedToDLQPayload {
  originalEventId: string;
  topic: string;
  reason: string;
  movedAt: string;
}
export type MessageMovedToDLQEvent = BaseEventEnvelope<MessageMovedToDLQPayload> & {
  eventType: typeof constants.MESSAGE_MOVED_TO_DLQ;
};

export interface ReplayRequestedPayload {
  correlationId: string;
  sagaId?: string;
  requestedAt: string;
}
export type ReplayRequestedEvent = BaseEventEnvelope<ReplayRequestedPayload> & {
  eventType: typeof constants.REPLAY_REQUESTED;
};

export interface ReplayCompletedPayload {
  correlationId: string;
  sagaId?: string;
  completedAt: string;
}
export type ReplayCompletedEvent = BaseEventEnvelope<ReplayCompletedPayload> & {
  eventType: typeof constants.REPLAY_COMPLETED;
};
