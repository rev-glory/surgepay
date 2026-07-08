export interface PrismaQueryClient {
  $queryRawUnsafe: <T = Record<string, unknown>[]>(query: string, ...values: unknown[]) => Promise<T>;
}

export async function assertPaymentPersisted(
  prisma: PrismaQueryClient,
  criteria: {
    merchantId: string;
    amount: number;
    currency: string;
    reference: string;
    status: string;
    requestId: string;
    correlationId: string;
    causationId: string;
  },
): Promise<Record<string, unknown>> {
  const result = await prisma.$queryRawUnsafe<Record<string, unknown>[]>(
    `SELECT * FROM "payment"."Payment" WHERE "merchantId" = $1::uuid AND reference = $2;`,
    criteria.merchantId,
    criteria.reference,
  );

  expect(result.length).toBe(1);
  const payment = result[0];
  if (!payment) {
    throw new Error('Expected payment record to exist in database.');
  }

  expect(payment.amount).toBe(criteria.amount);
  expect(payment.currency).toBe(criteria.currency);
  expect(payment.status).toBe(criteria.status);
  expect(payment.requestId).toBe(criteria.requestId);
  expect(payment.correlationId).toBe(criteria.correlationId);
  expect(payment.causationId).toBe(criteria.causationId);
  expect(payment.id).toBeDefined();
  expect(payment.createdAt).toBeInstanceOf(Date);
  expect(payment.updatedAt).toBeInstanceOf(Date);
  return payment;
}

export async function assertOutboxPersisted(
  prisma: PrismaQueryClient,
  criteria: {
    aggregateId: string;
    eventType: string;
    status: string;
    requestId: string;
    correlationId: string;
    causationId: string;
    payloadSchema?: Record<string, unknown>;
  },
): Promise<Record<string, unknown>> {
  const result = await prisma.$queryRawUnsafe<Record<string, unknown>[]>(
    `SELECT * FROM "payment"."OutboxEvent" WHERE "aggregateId" = $1::uuid;`,
    criteria.aggregateId,
  );

  expect(result.length).toBe(1);
  const outbox = result[0];
  if (!outbox) {
    throw new Error('Expected outbox record to exist in database.');
  }

  expect(outbox.aggregateType).toBe('Payment');
  expect(outbox.eventType).toBe(criteria.eventType);
  expect(outbox.status).toBe(criteria.status);
  expect(outbox.requestId).toBe(criteria.requestId);
  expect(outbox.correlationId).toBe(criteria.correlationId);
  expect(outbox.causationId).toBe(criteria.causationId);
  expect(outbox.createdAt).toBeInstanceOf(Date);

  if (criteria.payloadSchema) {
    const envelope = typeof outbox.payload === 'string' ? JSON.parse(outbox.payload) : outbox.payload;
    const businessPayload = (envelope && typeof envelope === 'object' && 'payload' in envelope)
      ? (envelope as Record<string, unknown>).payload
      : envelope;
    expect(businessPayload).toEqual(expect.objectContaining(criteria.payloadSchema));
  }
  return outbox;
}

export async function assertTransactionRollback(
  prisma: PrismaQueryClient,
  merchantId: string,
  reference: string,
  aggregateId?: string,
): Promise<void> {
  const payments = await prisma.$queryRawUnsafe<Record<string, unknown>[]>(
    `SELECT * FROM "payment"."Payment" WHERE "merchantId" = $1::uuid AND reference = $2;`,
    merchantId,
    reference,
  );

  let outboxEvents: Record<string, unknown>[] = [];
  if (aggregateId) {
    outboxEvents = await prisma.$queryRawUnsafe<Record<string, unknown>[]>(
      `SELECT * FROM "payment"."OutboxEvent" WHERE "aggregateId" = $1::uuid;`,
      aggregateId,
    );
  } else if (payments.length > 0) {
    const firstPayment = payments[0];
    if (firstPayment) {
      const paymentId = firstPayment.id;
      if (typeof paymentId === 'string') {
        outboxEvents = await prisma.$queryRawUnsafe<Record<string, unknown>[]>(
          `SELECT * FROM "payment"."OutboxEvent" WHERE "aggregateId" = $1::uuid;`,
          paymentId,
        );
      }
    }
  }

  const paymentExists = payments.length > 0;
  const outboxExists = outboxEvents.length > 0;

  expect(paymentExists).toBe(outboxExists);
  expect(paymentExists).toBe(false);
}
