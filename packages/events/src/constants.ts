// Payment Events
export const PAYMENT_INITIATED = 'PaymentInitiated';
export const PAYMENT_COMPLETED = 'PaymentCompleted';
export const PAYMENT_FAILED = 'PaymentFailed';
export const PAYMENT_REJECTED = 'PaymentRejected';
export const PAYMENT_FLAGGED = 'PaymentFlagged';

// Saga Commands
export const RECORD_LEDGER_ENTRY = 'RecordLedgerEntry';
export const CHECK_PAYOUT_ELIGIBILITY = 'CheckPayoutEligibility';
export const REVERSE_LEDGER_ENTRY = 'ReverseLedgerEntry';
export const RESERVE_BALANCE = 'ReserveBalance';
export const REVERSE_BALANCE = 'ReverseBalance';
export const NOTIFY_MERCHANT = 'NotifyMerchant';

// Ledger Events
export const LEDGER_ENTRY_RECORDED = 'LedgerEntryRecorded';
export const LEDGER_RECORDING_FAILED = 'LedgerRecordingFailed';
export const LEDGER_REVERSED = 'LedgerReversed';

// Risk Events
export const ELIGIBILITY_APPROVED = 'EligibilityApproved';
export const ELIGIBILITY_DENIED = 'EligibilityDenied';

// Balance Events
export const BALANCE_RESERVED = 'BalanceReserved';
export const BALANCE_RESERVATION_FAILED = 'BalanceReservationFailed';
export const BALANCE_REVERSED = 'BalanceReversed';

// Notification/Webhook Events
export const MERCHANT_NOTIFIED = 'MerchantNotified';
export const NOTIFICATION_DEFERRED = 'NotificationDeferred';
export const NOTIFICATION_DELIVERY_FAILED = 'NotificationDeliveryFailed';

// Operational Events
export const RETRY_SCHEDULED = 'RetryScheduled';
export const RETRY_EXHAUSTED = 'RetryExhausted';
export const MESSAGE_MOVED_TO_DLQ = 'MessageMovedToDLQ';
export const REPLAY_REQUESTED = 'ReplayRequested';
export const REPLAY_COMPLETED = 'ReplayCompleted';
