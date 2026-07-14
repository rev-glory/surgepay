import {
  CHECK_ORDER_ELIGIBILITY,
  CHECK_PAYOUT_ELIGIBILITY,
  LEDGER_ENTRY_RECORDED,
  LEDGER_RECORDING_FAILED,
  LEDGER_REVERSED,
  NOTIFY_MERCHANT,
  ORDER_ELIGIBILITY_CONFIRMED,
  ORDER_ELIGIBILITY_REJECTED,
  PAYMENT_COMPLETED,
  PAYMENT_INITIATED,
  RECORD_LEDGER_ENTRY,
  RESERVE_BALANCE,
  REVERSE_BALANCE,
  REVERSE_LEDGER_ENTRY,
  BALANCE_RESERVED,
  BALANCE_RESERVATION_FAILED,
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
  // Order Eligibility — roadmap-defined (Q3 Option A approved in implementation_plan.md).
  // Commands arrive on order.commands; result events are published to order.events.
  // Commit 6 Saga handler subscribes to order.events.
  [CHECK_ORDER_ELIGIBILITY]: 'order.commands',
  [ORDER_ELIGIBILITY_CONFIRMED]: 'order.events',
  [ORDER_ELIGIBILITY_REJECTED]: 'order.events',
  [LEDGER_ENTRY_RECORDED]: 'ledger.events',
  [LEDGER_RECORDING_FAILED]: 'ledger.events',
  [LEDGER_REVERSED]: 'ledger.events',
  [BALANCE_RESERVED]: 'balance.events',
  [BALANCE_RESERVATION_FAILED]: 'balance.events',
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
