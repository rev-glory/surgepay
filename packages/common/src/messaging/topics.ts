import { PAYMENT_INITIATED } from '@surgepay/events';

export const TOPIC_REGISTRY: Record<string, string> = {
  [PAYMENT_INITIATED]: 'payments.initiated',
};
