import { PAYMENT_INITIATED } from '@surgepay/events';

export const TOPIC_REGISTRY: Record<string, string> = {
  [PAYMENT_INITIATED]: 'payments.initiated',
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
