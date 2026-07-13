import { z } from 'zod';

export const kafkaSchema = z.object({
  KAFKA_BROKERS: z.string().min(1, 'KAFKA_BROKERS is required'),
  KAFKA_CLIENT_ID: z.string().default('surgepay'),
  KAFKA_SSL: z.preprocess((val) => val === 'true', z.boolean().default(false)),
  KAFKA_SASL: z.preprocess((val) => val === 'true', z.boolean().default(false)),
  KAFKA_CONSUMER_GROUP_ID: z.string().default('surgepay-consumer-group'),
});
