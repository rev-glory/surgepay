import { Test, TestingModule } from '@nestjs/testing';
import { Kafka, RecordMetadata, EachMessagePayload } from 'kafkajs';
import { LoggerService, MetricsService } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';
import { BaseEventEnvelope, EventEnvelope } from '@surgepay/events';
import {
  MessagingModule,
  ProducerService,
  BaseKafkaConsumer,
  InboxPersister,
  DuplicateEventException,
  KafkaDlqPublisher,
  KafkaEventHandler,
} from '@surgepay/common-messaging';
import { PrismaClient as PaymentPrismaClient } from '../../../apps/payment-service/src/generated/client';
import { PrismaClient as LedgerPrismaClient } from '@surgepay/database/generated/ledger';
import { PrismaService as PaymentPrismaService } from '../../../apps/outbox-relay/src/prisma.service';
import { RelayService } from '../../../apps/outbox-relay/src/relay.service';
import { RelayModule } from '../../../apps/outbox-relay/src/relay.module';
import { RedpandaTestContainer } from '../../testcontainers/redpanda.container';
import * as crypto from 'crypto';

class TestConsumer extends BaseKafkaConsumer {}

class TestInboxPersister implements InboxPersister {
  constructor(private readonly prisma: any, private readonly consumerName: string) {}

  async find(consumer: string, eventId: string): Promise<any | null> {
    const record = await this.prisma.inboxEvent.findUnique({
      where: {
        consumer_eventId: {
          consumer,
          eventId,
        },
      },
    });
    return record;
  }

  async persistReceived(envelope: EventEnvelope): Promise<any> {
    try {
      const record = await this.prisma.inboxEvent.create({
        data: {
          eventId: envelope.eventId,
          consumer: this.consumerName,
          eventType: envelope.eventType,
          status: 'RECEIVED',
          payload: envelope.payload as any,
          correlationId: envelope.correlationId,
          causationId: envelope.causationId,
          sagaId: envelope.sagaId ?? null,
          receivedAt: new Date(),
          retryCount: 0,
        },
      });
      return record;
    } catch (err: any) {
      if (err.code === 'P2002') {
        throw new DuplicateEventException(envelope.eventId, this.consumerName);
      }
      throw err;
    }
  }

  async markProcessing(id: string): Promise<void> {
    await this.prisma.inboxEvent.update({
      where: { id },
      data: { status: 'PROCESSING' },
    });
  }

  async markProcessed(id: string): Promise<void> {
    await this.prisma.inboxEvent.update({
      where: { id },
      data: { status: 'PROCESSED', processedAt: new Date() },
    });
  }

  async markFailed(id: string, reason: string): Promise<void> {
    await this.prisma.inboxEvent.update({
      where: { id },
      data: { status: 'FAILED', failureReason: reason },
    });
  }

  async markRetrying(id: string, reason: string): Promise<void> {
    await this.prisma.inboxEvent.update({
      where: { id },
      data: {
        status: 'RETRYING',
        retryCount: { increment: 1 },
        failureReason: reason,
      },
    });
  }

  async markDlqSent(id: string): Promise<void> {
    await this.prisma.inboxEvent.update({
      where: { id },
      data: { status: 'DLQ_SENT' },
    });
  }
}

async function pollUntil<T>(
  fn: () => Promise<T>,
  predicate: (val: T) => boolean,
  timeoutMs: number = 8000,
  intervalMs: number = 100
): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const val = await fn();
    if (predicate(val)) {
      return val;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`Polling timed out after ${timeoutMs}ms`);
}

async function fetchLastMessageFromTopic(kafka: Kafka, topic: string): Promise<any> {
  const consumer = kafka.consumer({ groupId: `temp-fetcher-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  
  let result: any = null;
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      consumer.disconnect().then(resolve).catch(reject);
    }, 8000);

    consumer.run({
      eachMessage: async ({ message }: EachMessagePayload) => {
        if (message.value) {
          try {
            result = JSON.parse(message.value.toString());
            clearTimeout(timeout);
            await consumer.disconnect();
            resolve();
          } catch (err) {
            reject(err);
          }
        }
      }
    }).catch(reject);
  });
  
  return result;
}

describe('Messaging System Integration', () => {
  let appModule: TestingModule;
  let producerService: ProducerService;
  let relayService: RelayService;
  let kafka: Kafka;
  let redpandaContainer: RedpandaTestContainer;

  const paymentDatabaseUrl = process.env.DATABASE_URL
    ? process.env.DATABASE_URL.replace('schema=merchant', 'schema=payment')
    : undefined;

  const ledgerDatabaseUrl = process.env.DATABASE_URL
    ? process.env.DATABASE_URL.replace('schema=merchant', 'schema=ledger')
    : undefined;

  const paymentPrisma = new PaymentPrismaClient({
    datasources: {
      db: {
        url: paymentDatabaseUrl,
      },
    },
  });

  const ledgerPrisma = new LedgerPrismaClient({
    datasources: {
      db: {
        url: ledgerDatabaseUrl,
      },
    },
  });

  beforeAll(async () => {
    // 1. Start Redpanda container for this test process
    redpandaContainer = new RedpandaTestContainer();
    const brokers = await redpandaContainer.start();
    process.env.KAFKA_BROKERS = brokers;

    appModule = await Test.createTestingModule({
      imports: [RelayModule, MessagingModule],
      providers: [
        LoggerService,
        MetricsService,
      ],
    }).compile();

    producerService = appModule.get(ProducerService);
    relayService = appModule.get(RelayService);

    await producerService.onModuleInit();

    kafka = new Kafka({
      clientId: 'integration-test-client',
      brokers: [brokers],
    });

    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic: 'payments.initiated' },
        { topic: 'saga.commands' },
        { topic: 'payment.dlq' },
      ],
    });
    await admin.disconnect();
  }, 60000);

  afterAll(async () => {
    await producerService.onModuleDestroy();
    await paymentPrisma.$disconnect();
    await ledgerPrisma.$disconnect();
    await appModule.close();
    await redpandaContainer.stop();
  }, 30000);

  beforeEach(async () => {
    await paymentPrisma.outboxEvent.deleteMany({});
    await paymentPrisma.payment.deleteMany({});
    await ledgerPrisma.inboxEvent.deleteMany({});
  });

  it('should successfully publish an event from Outbox to Kafka via OutboxRelay', async () => {
    const eventId = crypto.randomUUID();
    const correlationId = crypto.randomUUID();
    const causationId = crypto.randomUUID();
    const requestId = crypto.randomUUID();

    const envelope: BaseEventEnvelope<any> = {
      eventId,
      eventType: 'PaymentInitiated',
      correlationId,
      causationId,
      requestId,
      sagaId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      producer: 'payment-service',
      payload: {
        paymentId: crypto.randomUUID(),
        amount: 2500,
        currency: 'USD',
      },
    };

    const payment = await paymentPrisma.payment.create({
      data: {
        id: crypto.randomUUID(),
        merchantId: crypto.randomUUID(),
        amount: 2500,
        currency: 'USD',
        status: 'PENDING',
        reference: `ref_${Date.now()}`,
        requestId,
        correlationId,
        causationId,
        createdBy: 'test',
        source: 'integration-test',
      },
    });

    const outbox = await paymentPrisma.outboxEvent.create({
      data: {
        aggregateId: payment.id,
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: envelope as any,
        status: 'PENDING',
        requestId,
        correlationId,
        causationId,
      },
    });

    await relayService.runOnce();

    const updatedOutbox = await pollUntil(
      async () => await paymentPrisma.outboxEvent.findUnique({ where: { id: outbox.id } }),
      (row) => row?.status === 'PUBLISHED'
    );

    expect(updatedOutbox).toBeDefined();
    expect(updatedOutbox!.status).toBe('PUBLISHED');
    expect(updatedOutbox!.publishedAt).not.toBeNull();

    const receivedMsg = await fetchLastMessageFromTopic(kafka, 'payments.initiated');
    expect(receivedMsg).toBeDefined();
    expect(receivedMsg.eventId).toBe(eventId);
    expect(receivedMsg.correlationId).toBe(correlationId);
  }, 20000);

  it('should process a published event, persist in Inbox, and update status to PROCESSED', async () => {
    const eventId = crypto.randomUUID();
    const correlationId = crypto.randomUUID();
    const causationId = crypto.randomUUID();

    const envelope: BaseEventEnvelope<any> = {
      eventId,
      eventType: 'PaymentInitiated',
      correlationId,
      causationId,
      requestId: crypto.randomUUID(),
      sagaId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      producer: 'payment-service',
      payload: { paymentId: crypto.randomUUID(), amount: 1000 },
    };

    let executedCount = 0;
    const handler: KafkaEventHandler = {
      handle: async () => {
        executedCount++;
      },
    };

    const logger = appModule.get(LoggerService);
    const metrics = appModule.get(MetricsService);
    const dlqPublisher = appModule.get(KafkaDlqPublisher);
    const persister = new TestInboxPersister(ledgerPrisma, 'ledger-service');

    const consumer = new TestConsumer(
      kafka,
      persister,
      handler,
      dlqPublisher,
      logger,
      metrics,
      {
        groupId: 'ledger-service',
        topics: ['payments.initiated'],
        dlqTopic: 'payment.dlq',
        maxRetries: 3,
      }
    );

    await consumer.connect();

    try {
      await producerService.publish('payments.initiated', envelope);

      const inboxRecord = await pollUntil(
        async () => await ledgerPrisma.inboxEvent.findFirst({ where: { eventId } }),
        (row) => row?.status === 'PROCESSED'
      );

      expect(inboxRecord).toBeDefined();
      expect(inboxRecord!.status).toBe('PROCESSED');
      expect(executedCount).toBe(1);
    } finally {
      await consumer.disconnect();
    }
  }, 20000);

  it('should process duplicate event delivery exactly once and suppress the duplicate execution', async () => {
    const eventId = crypto.randomUUID();
    const correlationId = crypto.randomUUID();
    const causationId = crypto.randomUUID();

    const envelope: BaseEventEnvelope<any> = {
      eventId,
      eventType: 'PaymentInitiated',
      correlationId,
      causationId,
      requestId: crypto.randomUUID(),
      sagaId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      producer: 'payment-service',
      payload: { paymentId: crypto.randomUUID(), amount: 1200 },
    };

    let executedCount = 0;
    const handler: KafkaEventHandler = {
      handle: async () => {
        executedCount++;
      },
    };

    const logger = appModule.get(LoggerService);
    const metrics = appModule.get(MetricsService);
    const dlqPublisher = appModule.get(KafkaDlqPublisher);
    const persister = new TestInboxPersister(ledgerPrisma, 'duplicate-service');

    const consumer = new TestConsumer(
      kafka,
      persister,
      handler,
      dlqPublisher,
      logger,
      metrics,
      {
        groupId: 'duplicate-service',
        topics: ['payments.initiated'],
        dlqTopic: 'payment.dlq',
        maxRetries: 3,
      }
    );

    await consumer.connect();

    try {
      await producerService.publish('payments.initiated', envelope);

      await pollUntil(
        async () => await ledgerPrisma.inboxEvent.findFirst({ where: { eventId } }),
        (row) => row?.status === 'PROCESSED'
      );

      expect(executedCount).toBe(1);

      await producerService.publish('payments.initiated', envelope);

      await new Promise((resolve) => setTimeout(resolve, 1000));
      expect(executedCount).toBe(1);

      const count = await ledgerPrisma.inboxEvent.count({ where: { eventId, consumer: 'duplicate-service' } });
      expect(count).toBe(1);
    } finally {
      await consumer.disconnect();
    }
  }, 20000);

  it('should handle broker failure gracefully during outbox polling and succeed after recovery', async () => {
    const eventId = crypto.randomUUID();
    const correlationId = crypto.randomUUID();
    const causationId = crypto.randomUUID();
    const requestId = crypto.randomUUID();

    const envelope: BaseEventEnvelope<any> = {
      eventId,
      eventType: 'PaymentInitiated',
      correlationId,
      causationId,
      requestId,
      sagaId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      producer: 'payment-service',
      payload: { paymentId: crypto.randomUUID(), amount: 3000 },
    };

    const payment = await paymentPrisma.payment.create({
      data: {
        id: crypto.randomUUID(),
        merchantId: crypto.randomUUID(),
        amount: 3000,
        currency: 'USD',
        status: 'PENDING',
        reference: `ref_fail_${Date.now()}`,
        requestId,
        correlationId,
        causationId,
        createdBy: 'test',
        source: 'integration-test',
      },
    });

    const outbox = await paymentPrisma.outboxEvent.create({
      data: {
        aggregateId: payment.id,
        aggregateType: 'Payment',
        eventType: 'PaymentInitiated',
        payload: envelope as any,
        status: 'PENDING',
        requestId,
        correlationId,
        causationId,
      },
    });

    // 1. Stop Redpanda container to simulate broker failure
    await producerService.disconnect();
    await redpandaContainer.stop();

    // 2. Poll Outbox Relay: publish should fail
    try {
      await relayService.runOnce();
    } catch (err) {
      // Expected failure
    }

    const failedOutbox = await paymentPrisma.outboxEvent.findUnique({ where: { id: outbox.id } });
    expect(failedOutbox).toBeDefined();
    expect(['FAILED', 'RETRYING']).toContain(failedOutbox!.status);

    // 3. Restart Redpanda container
    const brokers = await redpandaContainer.start();
    process.env.KAFKA_BROKERS = brokers;

    await new Promise((resolve) => setTimeout(resolve, 10000));

    // Reconnect producer
    await producerService.connect();

    // 4. Poll Outbox Relay again: should now succeed
    await relayService.runOnce();

    const recoveredOutbox = await pollUntil(
      async () => await paymentPrisma.outboxEvent.findUnique({ where: { id: outbox.id } }),
      (row) => row?.status === 'PUBLISHED'
    );

    expect(recoveredOutbox).toBeDefined();
    expect(recoveredOutbox!.status).toBe('PUBLISHED');
  }, 40000);

  it('should route event to DLQ after maximum retry attempts are exhausted', async () => {
    const eventId = crypto.randomUUID();
    const correlationId = crypto.randomUUID();
    const causationId = crypto.randomUUID();

    const envelope: BaseEventEnvelope<any> = {
      eventId,
      eventType: 'PaymentInitiated',
      correlationId,
      causationId,
      requestId: crypto.randomUUID(),
      sagaId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      producer: 'payment-service',
      payload: { paymentId: crypto.randomUUID(), amount: 9999 },
    };

    let executedCount = 0;
    const handler: KafkaEventHandler = {
      handle: async () => {
        executedCount++;
        throw new Error('Permanent test error for DLQ');
      },
    };

    const logger = appModule.get(LoggerService);
    const metrics = appModule.get(MetricsService);
    const dlqPublisher = appModule.get(KafkaDlqPublisher);
    const persister = new TestInboxPersister(ledgerPrisma, 'dlq-test-service');

    const consumer = new TestConsumer(
      kafka,
      persister,
      handler,
      dlqPublisher,
      logger,
      metrics,
      {
        groupId: 'dlq-test-service',
        topics: ['payments.initiated'],
        dlqTopic: 'payment.dlq',
        maxRetries: 2,
      }
    );

    await consumer.connect();

    try {
      await producerService.publish('payments.initiated', envelope);

      const inboxRecord = await pollUntil(
        async () => await ledgerPrisma.inboxEvent.findFirst({ where: { eventId } }),
        (row) => row?.status === 'DLQ_SENT'
      );

      expect(inboxRecord).toBeDefined();
      expect(inboxRecord!.status).toBe('DLQ_SENT');
      expect(inboxRecord!.retryCount).toBe(2);
      expect(executedCount).toBe(3);

      const dlqMsg = await fetchLastMessageFromTopic(kafka, 'payment.dlq');
      expect(dlqMsg).toBeDefined();
      expect(dlqMsg.payload).toBeDefined();
      expect(dlqMsg.payload.originalEvent.eventId).toBe(eventId);
    } finally {
      await consumer.disconnect();
    }
  }, 30000);
});
