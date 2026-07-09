export const TOPICS = {
  PAYMENTS_INITIATED: 'payments.initiated',
  SAGA_COMMANDS: 'saga.commands',
  PAYMENT_DLQ: 'payment.dlq',
} as const;

export type Topic = typeof TOPICS[keyof typeof TOPICS];

export const isValidTopic = (topic: string): topic is Topic => {
  return Object.values(TOPICS).includes(topic as Topic);
};
