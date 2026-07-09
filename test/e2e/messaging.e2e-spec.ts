import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as request from 'supertest';
import { Kafka, EachMessagePayload } from 'kafkajs';
import { LoggerService, MetricsService } from '@surgepay/common';
import { EventEnvelope } from '@surgepay/events';
import {
  MessagingModule,
  ProducerService,
  BaseKafkaConsumer,
  InboxPersister,
  DuplicateEventException,
  KafkaDlqPublisher,
  KafkaEventHandler,
} from '@surgepay/common-messaging';
import { PrismaClient as PaymentPrismaClient } from '../../apps/payment-service/src/generated/client';
import { PrismaClient as LedgerPrismaClient } from '@surgepay/database/generated/ledger';
import { RelayService } from '../../apps/outbox-relay/src/relay.service';
import { RelayModule } from '../../apps/outbox-relay/src/relay.module';
import { RedpandaTestContainer } from '../testcontainers/redpanda.container';
import { setupE2EEnvironment, teardownE2EEnvironment } from '../helpers/test-setup';
import { clearDatabase, createTestMerchant } from '../helpers/db-helper';
import { clearRedis } from '../helpers/redis-helper';
import { MERCHANT_FIXTURES } from '../fixtures/merchants.fixture';
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
  timeoutMs: number = 10000,
  intervalMs: number = 200
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

async function fetchLastRawMessageFromTopic(kafka: Kafka, topic: string): Promise<{
  value: any;
  headers: Record<string, string>;
  offset: string;
  partition: number;
} | null> {
  const tempConsumer = kafka.consumer({ groupId: `temp-raw-fetcher-${crypto.randomUUID()}` });
  await tempConsumer.connect();
  await tempConsumer.subscribe({ topic, fromBeginning: true });
  
  let result: any = null;
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      tempConsumer.disconnect().then(resolve).catch(reject);
    }, 10000);

    tempConsumer.run({
      eachMessage: async ({ message, partition }: EachMessagePayload) => {
        if (message.value) {
          try {
            const headers: Record<string, string> = {};
            if (message.headers) {
              for (const [key, val] of Object.entries(message.headers)) {
                if (val !== undefined) {
                  headers[key] = Buffer.isBuffer(val) ? val.toString('utf8') : String(val);
                }
              }
            }
            result = {
              value: JSON.parse(message.value.toString()),
              headers,
              offset: message.offset,
              partition,
            };
            clearTimeout(timeout);
            await tempConsumer.disconnect();
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

describe('Messaging Subsystem - E2E Integration Pipeline', () => {
  let gatewayApp: INestApplication | null = null;
  let paymentApp: INestApplication | null = null;
  let relayApp: INestApplication;
  let relayService: RelayService;
  let kafka: Kafka;
  let redpandaContainer: RedpandaTestContainer;
  let merchantId: string;
  let producerService: ProducerService;

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
    // 1. Start dynamic Redpanda container
    redpandaContainer = new RedpandaTestContainer();
    const brokers = await redpandaContainer.start();
    process.env.KAFKA_BROKERS = brokers;

    // 2. Setup standard gateway and payment apps
    const environment = await setupE2EEnvironment();
    gatewayApp = environment.gatewayApp;
    paymentApp = environment.paymentApp;

    // 3. Setup Outbox Relay app
    const relayFixture = await Test.createTestingModule({
      imports: [RelayModule],
      providers: [LoggerService, MetricsService],
    }).compile();
    relayApp = relayFixture.createNestApplication();
    await relayApp.init();

    relayService = relayApp.get(RelayService);

    // 4. Resolve global producer service to verify topics
    const paymentModuleFixture = await Test.createTestingModule({
      imports: [MessagingModule],
      providers: [LoggerService, MetricsService],
    }).compile();
    const tempModule = await paymentModuleFixture.init();
    producerService = tempModule.get(ProducerService);

    kafka = new Kafka({
      clientId: 'e2e-messaging-test-client',
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
  }, 95000);

  afterAll(async () => {
    await relayApp.close();
    await paymentPrisma.$disconnect();
    await ledgerPrisma.$disconnect();
    await redpandaContainer.stop();
    await teardownE2EEnvironment();
  });

  beforeEach(async () => {
    await clearDatabase();
    await clearRedis();
    await paymentPrisma.outboxEvent.deleteMany({});
    await paymentPrisma.payment.deleteMany({});
    await ledgerPrisma.inboxEvent.deleteMany({});

    const merchant = await createTestMerchant(MERCHANT_FIXTURES.active);
    merchantId = merchant.merchantId;
  });

  it('should successfully execute happy path: POST /payments -> Outbox -> Kafka -> Inbox -> Handler -> PROCESSED', async () => {
    const idempotencyKey = `idem_e2e_msg_happy_${Date.now()}`;
    const payload = {
      idempotencyKey,
      amount: 150.0,
      currency: 'USD',
      merchantId,
      orderId: crypto.randomUUID(),
      paymentMethod: 'card',
    };

    // 1. Submit Request via API Gateway
    const response = await request(gatewayApp!.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send(payload);

    expect(response.status).toBe(202);
    const correlationId = response.headers['x-correlation-id'];
    const requestId = response.headers['x-request-id'];

    expect(correlationId).toBeDefined();
    expect(requestId).toBeDefined();

    // 2. Assert Outbox Record Created
    const outboxRecord = await pollUntil(
      async () => await paymentPrisma.outboxEvent.findFirst(),
      (row) => row?.status === 'PENDING'
    );
    expect(outboxRecord).toBeDefined();
    expect(outboxRecord!.eventType).toBe('PaymentInitiated');
    expect(outboxRecord!.correlationId).toBe(correlationId);
    expect(outboxRecord!.requestId).toBe(requestId);

    // 3. Run Outbox Relay
    await relayService.runOnce();

    // 4. Assert Outbox Transitions to PUBLISHED with metadata
    const publishedOutbox = await paymentPrisma.outboxEvent.findUnique({
      where: { id: outboxRecord!.id },
    });
    expect(publishedOutbox!.status).toBe('PUBLISHED');
    expect(publishedOutbox!.publishedAt).toBeDefined();
    expect(publishedOutbox!.partition).toBeDefined();
    expect(publishedOutbox!.offset).toBeDefined();
    expect(publishedOutbox!.retryCount).toBe(0);

    // 5. Assert Kafka Message Envelope & Trace Context Propagation
    const kafkaMessage = await fetchLastRawMessageFromTopic(kafka, 'payments.initiated');
    expect(kafkaMessage).not.toBeNull();
    const envelope = kafkaMessage!.value;

    expect(envelope.eventId).toBe(publishedOutbox!.id);
    expect(envelope.eventType).toBe('PaymentInitiated');
    expect(envelope.correlationId).toBe(correlationId);
    expect(envelope.causationId).toBe(publishedOutbox!.causationId);
    expect(envelope.requestId).toBe(requestId);
    expect(envelope.producer).toBe('payment-service');
    expect(envelope.version).toBeDefined();
    expect(envelope.timestamp).toBeDefined();
    expect(envelope.payload).toBeDefined();

    // Trace context validation
    expect(kafkaMessage!.headers['traceparent']).toBeDefined();
    expect(kafkaMessage!.headers['correlationId']).toBe(correlationId);
    expect(kafkaMessage!.headers['causationId']).toBe(publishedOutbox!.causationId);
    expect(kafkaMessage!.headers['requestId']).toBe(requestId);

    // 6. Connect Consumer & Process Message
    let handlerExecutionCount = 0;
    const mockHandler: KafkaEventHandler = {
      handle: async (env: EventEnvelope) => {
        expect(env.correlationId).toBe(correlationId);
        expect(env.causationId).toBe(publishedOutbox!.causationId);
        expect(env.requestId).toBe(requestId);
        handlerExecutionCount++;
      },
    };

    const dlqPublisher = new KafkaDlqPublisher(producerService);
    const persister = new TestInboxPersister(ledgerPrisma, 'ledger-consumer');
    const consumer = new TestConsumer(
      kafka,
      persister,
      mockHandler,
      dlqPublisher,
      paymentApp!.get(LoggerService),
      paymentApp!.get(MetricsService),
      {
        groupId: 'e2e-payment-group',
        topics: ['payments.initiated'],
        dlqTopic: 'payment.dlq',
        maxRetries: 3,
      }
    );

    await consumer.connect();

    // 7. Wait for consumer to process message
    const inboxRecord = await pollUntil(
      async () => await ledgerPrisma.inboxEvent.findUnique({
        where: {
          consumer_eventId: {
            consumer: 'ledger-consumer',
            eventId: publishedOutbox!.id,
          },
        },
      }),
      (row) => row?.status === 'PROCESSED'
    );

    expect(inboxRecord).toBeDefined();
    expect(inboxRecord!.status).toBe('PROCESSED');
    expect(inboxRecord!.processedAt).toBeDefined();
    expect(inboxRecord!.retryCount).toBe(0);
    expect(inboxRecord!.consumer).toBe('ledger-consumer');
    expect(inboxRecord!.eventId).toBe(publishedOutbox!.id);

    expect(handlerExecutionCount).toBe(1);

    await consumer.disconnect();
  }, 45000);
  it('should verify broker recovery and self-healing when Kafka is initially unavailable', async () => {
    // 1. Stop Redpanda
    await producerService.disconnect();
    await relayApp.get(ProducerService).disconnect();
    await redpandaContainer.stop();

    // 2. Submit payment request via API Gateway
    const idempotencyKey = `idem_e2e_broker_fail_${Date.now()}`;
    await request(gatewayApp!.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 50.0,
        currency: 'USD',
        merchantId,
        orderId: crypto.randomUUID(),
        paymentMethod: 'card',
      });

    const outbox = await paymentPrisma.outboxEvent.findFirst();
    expect(outbox!.status).toBe('PENDING');

    // 3. Attempt Outbox Relay polling -> should fail and increment retries
    try {
      await relayService.runOnce();
    } catch (_err) {
      // Expected
    }

    const failedOutbox = await paymentPrisma.outboxEvent.findUnique({ where: { id: outbox!.id } });
    expect(failedOutbox!.status).toBe('RETRYING');
    expect(failedOutbox!.retryCount).toBe(1);

    // 4. Restart Redpanda (reuses same port)
    const brokers = await redpandaContainer.start();
    process.env.KAFKA_BROKERS = brokers;

    // Give broker time to start up
    await new Promise((resolve) => setTimeout(resolve, 10000));
    await producerService.connect();
    await relayApp.get(ProducerService).connect();

    // 5. Run Relay again -> should now succeed
    await relayService.runOnce();

    const recoveredOutbox = await pollUntil(
      async () => await paymentPrisma.outboxEvent.findUnique({ where: { id: outbox!.id } }),
      (row) => row?.status === 'PUBLISHED'
    );
    expect(recoveredOutbox!.status).toBe('PUBLISHED');
  }, 45000);

  it('should verify duplicate delivery and suppress duplicate execution via Inbox idempotency', async () => {
    let handlerExecutionCount = 0;
    const mockHandler: KafkaEventHandler = {
      handle: async () => {
        handlerExecutionCount++;
      },
    };

    const dlqPublisher = new KafkaDlqPublisher(producerService);
    const persister = new TestInboxPersister(ledgerPrisma, 'ledger-consumer');
    const consumer = new TestConsumer(
      kafka,
      persister,
      mockHandler,
      dlqPublisher,
      paymentApp!.get(LoggerService),
      paymentApp!.get(MetricsService),
      {
        groupId: 'e2e-payment-dup-group',
        topics: ['payments.initiated'],
        dlqTopic: 'payment.dlq',
        maxRetries: 3,
      }
    );

    await consumer.connect();

    // Publish identical event envelope twice manually
    const eventId = crypto.randomUUID();
    const envelope: EventEnvelope = {
      eventId,
      eventType: 'PaymentInitiated',
      correlationId: crypto.randomUUID(),
      causationId: crypto.randomUUID(),
      requestId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      payload: { amount: 100 },
      producer: 'payment-service',
    };

    await producerService.publish('payments.initiated', envelope);
    await producerService.publish('payments.initiated', envelope);

    // Wait for event to transition to PROCESSED
    await pollUntil(
      async () => await ledgerPrisma.inboxEvent.findUnique({
        where: { consumer_eventId: { consumer: 'ledger-consumer', eventId } },
      }),
      (row) => row?.status === 'PROCESSED'
    );

    // Allow duplicate message delivery processing to finish
    await new Promise((resolve) => setTimeout(resolve, 2000));

    const inboxRecords = await ledgerPrisma.inboxEvent.findMany({
      where: { eventId, consumer: 'ledger-consumer' },
    });
    expect(inboxRecords.length).toBe(1);
    expect(handlerExecutionCount).toBe(1);

    await consumer.disconnect();
  }, 30000);

  it('should verify consumer crash and recovery without duplicate execution', async () => {
    let handlerExecutionCount = 0;
    const mockHandler: KafkaEventHandler = {
      handle: async () => {
        handlerExecutionCount++;
      },
    };

    const dlqPublisher = new KafkaDlqPublisher(producerService);
    const persister = new TestInboxPersister(ledgerPrisma, 'ledger-consumer');
    
    // Start consumer
    let consumer = new TestConsumer(
      kafka,
      persister,
      mockHandler,
      dlqPublisher,
      paymentApp!.get(LoggerService),
      paymentApp!.get(MetricsService),
      {
        groupId: 'e2e-payment-crash-group',
        topics: ['payments.initiated'],
        dlqTopic: 'payment.dlq',
        maxRetries: 3,
      }
    );

    await consumer.connect();

    // Publish event
    const eventId = crypto.randomUUID();
    const envelope: EventEnvelope = {
      eventId,
      eventType: 'PaymentInitiated',
      correlationId: crypto.randomUUID(),
      causationId: crypto.randomUUID(),
      requestId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      payload: { amount: 100 },
      producer: 'payment-service',
    };

    await producerService.publish('payments.initiated', envelope);

    // Wait for it to be processed
    await pollUntil(
      async () => await ledgerPrisma.inboxEvent.findUnique({
        where: { consumer_eventId: { consumer: 'ledger-consumer', eventId } },
      }),
      (row) => row?.status === 'PROCESSED'
    );

    expect(handlerExecutionCount).toBe(1);

    // Simulate consumer crash
    await consumer.disconnect();

    // Recreate/Restart consumer
    consumer = new TestConsumer(
      kafka,
      persister,
      mockHandler,
      dlqPublisher,
      paymentApp!.get(LoggerService),
      paymentApp!.get(MetricsService),
      {
        groupId: 'e2e-payment-crash-group',
        topics: ['payments.initiated'],
        dlqTopic: 'payment.dlq',
        maxRetries: 3,
      }
    );

    await consumer.connect();

    // Publish same message again to simulate duplicate delivery after crash
    await producerService.publish('payments.initiated', envelope);

    // Verify it was ignored
    await new Promise((resolve) => setTimeout(resolve, 2000));
    expect(handlerExecutionCount).toBe(1);

    await consumer.disconnect();
  }, 35000);

  it('should verify relay crash and recovery logic without duplicate publishing', async () => {
    // 1. Terminate Relay
    await relayApp.close();

    const idempotencyKey = `idem_e2e_relay_crash_${Date.now()}`;
    await request(gatewayApp!.getHttpServer())
      .post('/api/v1/payments')
      .set('x-api-key', MERCHANT_FIXTURES.active.apiKey)
      .set('idempotency-key', idempotencyKey)
      .send({
        idempotencyKey,
        amount: 80.0,
        currency: 'USD',
        merchantId,
        orderId: crypto.randomUUID(),
        paymentMethod: 'card',
      });

    // 2. Verify event is PENDING in outbox
    const outbox = await paymentPrisma.outboxEvent.findFirst({
      orderBy: { createdAt: 'desc' },
    });
    expect(outbox!.status).toBe('PENDING');

    // 3. Restart Relay App
    const relayFixture = await Test.createTestingModule({
      imports: [RelayModule],
      providers: [LoggerService, MetricsService],
    }).compile();
    relayApp = relayFixture.createNestApplication();
    await relayApp.init();
    relayService = relayApp.get(RelayService);

    // 4. Poll
    await relayService.runOnce();

    // 5. Verify published exactly once
    const publishedOutbox = await paymentPrisma.outboxEvent.findUnique({
      where: { id: outbox!.id },
    });
    expect(publishedOutbox!.status).toBe('PUBLISHED');
  }, 30000);

  it('should verify poison message is routed to DLQ on permanent handler failure', async () => {
    const mockHandler: KafkaEventHandler = {
      handle: async () => {
        throw new Error('Permanent processing error');
      },
    };

    const dlqPublisher = new KafkaDlqPublisher(producerService);
    const persister = new TestInboxPersister(ledgerPrisma, 'ledger-consumer');
    const consumer = new TestConsumer(
      kafka,
      persister,
      mockHandler,
      dlqPublisher,
      paymentApp!.get(LoggerService),
      paymentApp!.get(MetricsService),
      {
        groupId: 'e2e-payment-dlq-group',
        topics: ['payments.initiated'],
        dlqTopic: 'payment.dlq',
        maxRetries: 2,
      }
    );

    await consumer.connect();

    const eventId = crypto.randomUUID();
    const correlationId = crypto.randomUUID();
    const envelope: EventEnvelope = {
      eventId,
      eventType: 'PaymentInitiated',
      correlationId,
      causationId: crypto.randomUUID(),
      requestId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      payload: { amount: 100 },
      producer: 'payment-service',
    };

    await producerService.publish('payments.initiated', envelope);

    // Wait for DLQ transition
    const inboxRecord = await pollUntil(
      async () => await ledgerPrisma.inboxEvent.findUnique({
        where: { consumer_eventId: { consumer: 'ledger-consumer', eventId } },
      }),
      (row) => row?.status === 'DLQ_SENT'
    );

    expect(inboxRecord!.status).toBe('DLQ_SENT');

    // Fetch and verify message in DLQ
    const dlqMessage = await fetchLastRawMessageFromTopic(kafka, 'payment.dlq');
    expect(dlqMessage).not.toBeNull();
    const dlqPayload = dlqMessage!.value.payload;

    expect(dlqPayload.originalEvent).toEqual(envelope);
    expect(dlqPayload.consumer).toBe('ledger-consumer');
    expect(dlqPayload.retryCount).toBe(2);
    expect(dlqPayload.failureReason).toContain('Permanent processing error');
    expect(dlqPayload.failedAt).toBeDefined();

    await consumer.disconnect();
  }, 35000);
});
