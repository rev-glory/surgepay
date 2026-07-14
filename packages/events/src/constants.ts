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

// Order Eligibility — roadmap-defined extension (commits.txt Commit 5, lines 7018–7076)
// Architecture note: CheckOrderEligibility, OrderEligibilityConfirmed, and
// OrderEligibilityRejected are absent from doc-v3 §9.4 Saga Commands and §9 event sections.
// They are defined by the approved build roadmap and have received explicit architectural
// approval (implementation_plan.md Open Question 1, Option C). This comment marks the gap
// for a future doc-v3 §9.4 amendment.
export const CHECK_ORDER_ELIGIBILITY = 'CheckOrderEligibility';
export const ORDER_ELIGIBILITY_CONFIRMED = 'OrderEligibilityConfirmed';
export const ORDER_ELIGIBILITY_REJECTED = 'OrderEligibilityRejected';

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
