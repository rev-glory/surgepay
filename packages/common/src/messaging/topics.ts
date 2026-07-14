import {
  CHECK_PAYOUT_ELIGIBILITY,
  NOTIFY_MERCHANT,
  PAYMENT_COMPLETED,
  PAYMENT_INITIATED,
  RECORD_LEDGER_ENTRY,
  RESERVE_BALANCE,
  REVERSE_BALANCE,
  REVERSE_LEDGER_ENTRY,
} from '@surgepay/events';

export const TOPIC_REGISTRY: Record<string, string> = {
  [PAYMENT_INITIATED]: 'payments.initiated',
  [PAYMENT_COMPLETED]: 'payments.completed',
  [RECORD_LEDGER_ENTRY]: 'ledger.commands',
  [REVERSE_LEDGER_ENTRY]: 'ledger.commands',
  [CHECK_PAYOUT_ELIGIBILITY]: 'risk.commands',
  [RESERVE_BALANCE]: 'balance.commands',
  [REVERSE_BALANCE]: 'balance.commands',
  [NOTIFY_MERCHANT]: 'notification.commands',
  'payments.dlq': 'payments.dlq',
};

/**
 * Resolves the Dead Letter Queue (DLQ) topic.
 * Under doc-v3 Section 3.14, SurgePay routes all failed consumer operations to a single
 * canonical DLQ topic 'payments.dlq'.
 */
export function resolveDlqTopic(): string {
  return 'payments.dlq';
}
