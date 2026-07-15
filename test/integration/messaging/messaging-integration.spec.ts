import { randomUUID } from 'crypto';
import { execSync } from 'child_process';
import { Test, type TestingModule } from '@nestjs/testing';
import { propagation } from '@opentelemetry/api';
import { Kafka, EachMessagePayload } from 'kafkajs';

import { BaseEventEnvelope, PAYMENT_INITIATED, CHECK_ORDER_ELIGIBILITY } from '@surgepay/events';
import { ConfigService } from '@surgepay/config';
import { KafkaEventProducer, EventSerializer } from '@surgepay/common';

// Outbox / Relay imports
import { RelayModule } from '../../../apps/outbox-relay/src/relay.module';
import { OutboxRelayService } from '../../../apps/outbox-relay/src/relay.service';
import { OutboxRepository } from '../../../apps/outbox-relay/src/repositories/outbox.repository';
import { OutboxScheduler } from '../../../apps/outbox-relay/src/scheduler';
import { PrismaService as OutboxPrismaService } from '../../../apps/outbox-relay/src/prisma/prisma.service';
import { OutboxStatus } from '../../../apps/outbox-relay/src/generated/client';

// Order / Consumer / Inbox imports
import { OrderModule } from '../../../apps/order-service/src/modules/order.module';
import { PrismaModule as OrderPrismaModule } from '../../../apps/order-service/src/prisma/prisma.module';
import { PrismaService as OrderPrismaService } from '../../../apps/order-service/src/prisma/prisma.service';
import { OrderEventConsumer } from '../../../apps/order-service/src/services/order-event.consumer';
import { OrderInboxRepository } from '../../../apps/order-service/src/repositories/inbox.repository';

async function eventually(
  assertion: () => Promise<void> | void,
  timeoutMs = 15000,
  intervalMs = 150
): Promise<void> {
  const startTime = Date.now();
  while (true) {
    try {
      await assertion();
      return;
    } catch (error) {
      if (Date.now() - startTime > timeoutMs) {
        throw new Error(`eventually timed out after ${timeoutMs}ms. Last error: ${(error as Error).message}`);
      }
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }
}

// Configure mock OTel propagator for traceparent validation
const mockTraceParent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
const mockPropagator = {
  inject(ctx: any, carrier: any, setter: any) {
    setter.set(carrier, 'traceparent', mockTraceParent);
  },
  extract(ctx: any, carrier: any, getter: any) {
    return ctx;
  },
  fields() {
    return ['traceparent'];
  }
};
propagation.setGlobalPropagator(mockPropagator);

describe('Asynchronous Messaging Integration Spec', () => {
  let outboxFixture: TestingModule;
  let orderFixture: TestingModule;

  let outboxPrisma: OutboxPrismaService;
  let orderPrisma: OrderPrismaService;

  let outboxRelayService: OutboxRelayService;
  let orderEventConsumer: OrderEventConsumer;
  let orderInboxRepo: OrderInboxRepository;
  let configService: ConfigService;

  beforeAll(async () => {
    // Clean database connection schema string to allow dynamic schema allocation by getOrCreatePrismaClient
    const originalUrl = process.env.DATABASE_URL;
    if (originalUrl) {
      const url = new URL(originalUrl);
      url.searchParams.delete('schema');
      process.env.DATABASE_URL = url.toString();
    }

    try {
      // Boot Outbox Relay context with a mocked scheduler to prevent background ticks from race-conditioning the tests
      outboxFixture = await Test.createTestingModule({
        imports: [RelayModule],
      })
        .overrideProvider(OutboxScheduler)
        .useValue({
          onApplicationBootstrap: () => {},
          onApplicationShutdown: () => {},
        })
        .compile();

      // Boot Order Service context
      orderFixture = await Test.createTestingModule({
        imports: [OrderPrismaModule, OrderModule],
      }).compile();
    } finally {
      process.env.DATABASE_URL = originalUrl;
    }

    outboxPrisma = outboxFixture.get<OutboxPrismaService>(OutboxPrismaService);
    orderPrisma = orderFixture.get<OrderPrismaService>(OrderPrismaService);

    outboxRelayService = outboxFixture.get<OutboxRelayService>(OutboxRelayService);
    orderEventConsumer = orderFixture.get<OrderEventConsumer>(OrderEventConsumer);
    orderInboxRepo = orderFixture.get<OrderInboxRepository>(OrderInboxRepository);
    configService = outboxFixture.get<ConfigService>(ConfigService);

    await outboxPrisma.client.$connect();
    await orderPrisma.client.$connect();

    // Start Nest module lifecycles (which connects producers/consumers to Redpanda)
    await outboxFixture.init();
    await orderFixture.init();
  });

  afterAll(async () => {
    await outboxPrisma.client.$disconnect();
    await orderPrisma.client.$disconnect();

    await outboxFixture.close();
    await orderFixture.close();
  });

  beforeEach(async () => {
    jest.restoreAllMocks();
    // Clear outbox and inbox tables before each test for isolation
    await outboxPrisma.client.$executeRawUnsafe('TRUNCATE TABLE "payment"."OutboxEvent" CASCADE;');
    await orderPrisma.client.$executeRawUnsafe('TRUNCATE TABLE "order"."InboxEvent" CASCADE;');
  });

  function createTestEnvelope(eventId: string): BaseEventEnvelope<any> {
    return {
      eventId,
      eventType: PAYMENT_INITIATED,
      correlationId: randomUUID(),
      causationId: randomUUID(),
      sagaId: randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId: randomUUID(),
        amount: 25000,
        currency: 'USD',
        merchantId: randomUUID(),
        orderId: randomUUID(),
        paymentMethod: 'card',
      },
    };
  }

  // --- Scenario 1 & 2: Successful Outbox Publication & Kafka ACK Metadata ---
  it('should successfully publish an Outbox event to Kafka and store broker metadata in status PUBLISHED', async () => {
    const eventId = randomUUID();
    const envelope = createTestEnvelope(eventId);
    const aggregateId = randomUUID();

    // 1. Persist Outbox event in PG
    const outboxEvent = await outboxPrisma.client.outboxEvent.create({
      data: {
        id: eventId,
        aggregateId,
        aggregateType: 'Payment',
        eventType: envelope.eventType,
        payload: envelope as any,
        status: OutboxStatus.PENDING,
        requestId: randomUUID(),
        correlationId: envelope.correlationId,
        causationId: envelope.causationId,
        traceHeaders: {},
      },
    });

    expect(outboxEvent.status).toBe(OutboxStatus.PENDING);

    // 2. Setup standard Kafka consumer to verify broker-visible event
    const kafka = new Kafka({
      brokers: configService.kafka.brokers,
      clientId: 'test-scenario1-consumer',
    });
    const testConsumer = kafka.consumer({ groupId: `group-${randomUUID()}` });
    
    try {
      await testConsumer.connect();
      await testConsumer.subscribe({ topic: 'payments.initiated', fromBeginning: false });

      const consumedMessages: any[] = [];
      await testConsumer.run({
        eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
          consumedMessages.push({
            key: message.key?.toString(),
            value: message.value?.toString(),
            headers: message.headers,
          });
        },
      });

      // 3. Trigger Outbox Relay batch processing
      await outboxRelayService.processBatch();

      // 4. Assert Outbox event reaches PUBLISHED in database with partition, offset, and publishedAt set
      let updatedEvent: any;
      await eventually(async () => {
        updatedEvent = await outboxPrisma.client.outboxEvent.findUnique({
          where: { id: eventId },
        });
        expect(updatedEvent?.status).toBe(OutboxStatus.PUBLISHED);
        expect(updatedEvent?.partition).not.toBeNull();
        expect(updatedEvent?.offset).not.toBeNull();
        expect(updatedEvent?.publishedAt).not.toBeNull();
        expect(updatedEvent?.retryCount).toBe(0);
      });

      // 5. Assert event is consumed by our test consumer and has correct payload & envelope fields
      await eventually(async () => {
        const myMsg = consumedMessages.find((msg) => {
          try {
            const parsed = JSON.parse(msg.value);
            return parsed.eventId === eventId;
          } catch {
            return false;
          }
        });
        expect(myMsg).toBeDefined();
        expect(myMsg.key).toBe(aggregateId);

        const parsedEnvelope = JSON.parse(myMsg.value);
        expect(parsedEnvelope.eventId).toBe(eventId);
        expect(parsedEnvelope.eventType).toBe(PAYMENT_INITIATED);
        expect(parsedEnvelope.correlationId).toBe(envelope.correlationId);
        expect(parsedEnvelope.causationId).toBe(envelope.causationId);
        expect(parsedEnvelope.payload.amount).toBe(25000);
      });
    } finally {
      // Safe teardown
      await testConsumer.disconnect();
    }
  });

  // --- Scenario 3 & 4: Consumer Inbox Processing & Duplicate Delivery ---
  it('should process a consumed Kafka event, persist in Inbox as PROCESSED, execute handler, and suppress subsequent duplicates', async () => {
    const eventId = randomUUID();
    const envelope = {
      ...createTestEnvelope(eventId),
      eventType: CHECK_ORDER_ELIGIBILITY,
    };

    // Track handler executions
    let handlerExecutions = 0;
    const originalHandleEvent = (orderEventConsumer as any).handleEvent;
    
    // Inject a spy onto the private/protected handleEvent method (only count target eventId)
    jest.spyOn(orderEventConsumer as any, 'handleEvent').mockImplementation(async (env: any) => {
      if (env.eventId === eventId) {
        handlerExecutions++;
      }
      return originalHandleEvent.call(orderEventConsumer, env);
    });

    // 1. Manually publish the event to Kafka twice using a separate client
    const kafka = new Kafka({
      brokers: configService.kafka.brokers,
      clientId: 'test-scenario3-producer',
    });
    const testProducer = kafka.producer();
    
    try {
      await testProducer.connect();

      const serializedPayload = EventSerializer.serialize(envelope);
      
      // Publish first delivery
      await testProducer.send({
        topic: 'order.commands',
        messages: [
          {
            key: randomUUID(),
            value: serializedPayload,
          },
        ],
      });

      // 2. Verify Inbox gets created and transitions to PROCESSED
      await eventually(async () => {
        const inboxRecord = await orderInboxRepo.findByEventIdAndConsumer(eventId, orderEventConsumer['groupId']);
        expect(inboxRecord).not.toBeNull();
        expect(inboxRecord?.status).toBe('PROCESSED');
        expect(inboxRecord?.processedAt).not.toBeNull();
        expect(inboxRecord?.retryCount).toBe(0);
        expect(inboxRecord?.correlationId).toBe(envelope.correlationId);
        expect(inboxRecord?.causationId).toBe(envelope.causationId);
      });

      expect(handlerExecutions).toBe(1);

      // Publish second delivery (Duplicate)
      await testProducer.send({
        topic: 'order.commands',
        messages: [
          {
            key: randomUUID(),
            value: serializedPayload,
          },
        ],
      });

      // Wait a short duration and check that handler was not executed again
      await new Promise((resolve) => setTimeout(resolve, 1500));

      expect(handlerExecutions).toBe(1);

      // Verify exactly one inbox record exists
      const allRecords = await orderPrisma.client.inboxEvent.findMany({
        where: { eventId, consumer: orderEventConsumer['groupId'] },
      });
      expect(allRecords.length).toBe(1);
    } finally {
      await testProducer.disconnect();
    }
  });

  // --- Scenario 5: Broker Unavailability — No Durable Event Loss ---
  it('should not lose or delete durable Outbox events during broker failure, and correctly update retry/failed states', async () => {
    const eventId = randomUUID();
    const envelope = createTestEnvelope(eventId);
    const aggregateId = randomUUID();

    // 1. Persist Outbox event in PG
    await outboxPrisma.client.outboxEvent.create({
      data: {
        id: eventId,
        aggregateId,
        aggregateType: 'Payment',
        eventType: envelope.eventType,
        payload: envelope as any,
        status: OutboxStatus.PENDING,
        requestId: randomUUID(),
        correlationId: envelope.correlationId,
        causationId: envelope.causationId,
        traceHeaders: {},
      },
    });

    const containerId = process.env.REDPANDA_CONTAINER_ID;
    if (!containerId) {
      throw new Error('REDPANDA_CONTAINER_ID environment variable is not defined');
    }

    try {
      // 2. Cleanly disconnect the consumer while Redpanda is online to avoid connection retry/disconnect hang states later
      await orderEventConsumer.onModuleDestroy();

      // 3. Stop the running Redpanda container using Docker CLI immediately (-t 0)
      execSync(`docker stop -t 0 ${containerId}`);

      // 4. Trigger Outbox Relay batch processing (expecting error)
      await expect(outboxRelayService.processBatch()).rejects.toThrow();
    } finally {
      // 5. Start the Redpanda container again so subsequent tests can continue
      execSync(`docker start ${containerId}`);
      // Wait deterministically for Redpanda to resume and accept connections
      const healthStart = Date.now();
      let isHealthy = false;
      while (!isHealthy && Date.now() - healthStart < 15000) {
        try {
          execSync(`docker exec ${containerId} rpk cluster info`, { stdio: 'ignore' });
          isHealthy = true;
        } catch {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      }

      // 6. Cleanly restart the consumer now that the broker is online
      await orderEventConsumer.onModuleInit();
    }

    // 7. Assert the Outbox event still exists, is NOT deleted, is NOT marked PUBLISHED
    const updatedEvent = await outboxPrisma.client.outboxEvent.findUnique({
      where: { id: eventId },
    });

    expect(updatedEvent).not.toBeNull();
    // Verify it follows the Commit 3 failure transition
    const retryLimit = configService.outbox.retryLimit;
    if (updatedEvent!.retryCount < retryLimit) {
      expect(updatedEvent!.status).toBe(OutboxStatus.RETRYING);
    } else {
      expect(updatedEvent!.status).toBe(OutboxStatus.FAILED);
    }
    expect(updatedEvent!.retryCount).toBe(1);
    expect(updatedEvent!.publishedAt).toBeNull();
    expect(updatedEvent!.partition).toBeNull();
    expect(updatedEvent!.offset).toBeNull();
  }, 60000); // 60s timeout for container stop/start/re-init

  // --- Scenario 6: Poison Message and DLQ Routing ---
  it('should route permanently failing events to payments.dlq topic and mark Inbox as DLQ_SENT upon retry exhaustion', async () => {
    const eventId = randomUUID();
    const envelope = {
      ...createTestEnvelope(eventId),
      eventType: CHECK_ORDER_ELIGIBILITY,
    };

    // Setup failing handler (only mock throw for target eventId)
    const failingError = new Error('Deterministic poison message handler failure');
    const originalHandleEvent = (orderEventConsumer as any).handleEvent;
    jest.spyOn(orderEventConsumer as any, 'handleEvent').mockImplementation(async (env: any) => {
      if (env.eventId === eventId) {
        throw failingError;
      }
      return originalHandleEvent.call(orderEventConsumer, env);
    });

    // Get the configured consumer retry limit
    const retryLimit = configService.kafka.consumerRetryLimit;

    // Seed the Inbox record so that it is already at the limit
    await orderPrisma.client.inboxEvent.create({
      data: {
        eventId,
        consumer: orderEventConsumer['groupId'],
        status: 'RETRYING',
        retryCount: retryLimit,
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        causationId: envelope.causationId,
        sagaId: envelope.sagaId,
        timestamp: new Date(envelope.timestamp),
        version: envelope.version,
        payload: envelope.payload as any,
      },
    });

    // Setup standard DLQ consumer to verify DLQ message publication
    const kafka = new Kafka({
      brokers: configService.kafka.brokers,
      clientId: 'test-dlq-verifier-consumer',
    });
    const dlqConsumer = kafka.consumer({ groupId: `dlq-group-${randomUUID()}` });
    const testProducer = kafka.producer();
    
    try {
      await dlqConsumer.connect();
      await dlqConsumer.subscribe({ topic: 'payments.dlq', fromBeginning: false });

      const dlqMessages: any[] = [];
      await dlqConsumer.run({
        eachMessage: async ({ message }: EachMessagePayload) => {
          dlqMessages.push(JSON.parse(message.value!.toString()));
        },
      });

      // Publish event to Kafka to trigger consumer attempt
      await testProducer.connect();
      await testProducer.send({
        topic: 'order.commands',
        messages: [
          {
            key: randomUUID(),
            value: EventSerializer.serialize(envelope),
          },
        ],
      });

      // Assert Inbox status reaches DLQ_SENT with incremented retryCount = limit + 1
      await eventually(async () => {
        const updatedInbox = await orderInboxRepo.findByEventIdAndConsumer(eventId, orderEventConsumer['groupId']);
        expect(updatedInbox).not.toBeNull();
        expect(updatedInbox?.status).toBe('DLQ_SENT');
        expect(updatedInbox?.retryCount).toBe(retryLimit + 1);
      });

      // Assert event is published to DLQ with proper failure reason and original event info
      await eventually(async () => {
        const myDlqRecord = dlqMessages.find(
          (msg) => msg.payload?.originalEvent?.eventId === eventId
        );
        expect(myDlqRecord).toBeDefined();
        expect(myDlqRecord.eventType).toBe('DeadLetterRecord');
        expect(myDlqRecord.payload.originalEvent.eventId).toBe(eventId);
        expect(myDlqRecord.payload.failureReason).toContain(failingError.message);
        expect(myDlqRecord.payload.retryCount).toBe(retryLimit + 1);
        expect(myDlqRecord.payload.consumer).toBe(orderEventConsumer['groupId']);
        expect(myDlqRecord.payload.dlqTopic).toBe('payments.dlq');
      });
    } finally {
      await testProducer.disconnect();
      await dlqConsumer.disconnect();
    }
  });

  // --- Scenario 7: Trace Context Verification ---
  it('should propagate traceparent in Kafka message headers when published under an active trace context', async () => {
    const eventId = randomUUID();
    const envelope = createTestEnvelope(eventId);
    const aggregateId = randomUUID();

    // 1. Setup standard Kafka consumer to check published headers
    const kafka = new Kafka({
      brokers: configService.kafka.brokers,
      clientId: 'test-trace-verifier-consumer',
    });
    const traceConsumer = kafka.consumer({ groupId: `trace-group-${randomUUID()}` });
    
    try {
      await traceConsumer.connect();
      await traceConsumer.subscribe({ topic: 'payments.initiated', fromBeginning: false });

      const consumedMessages: any[] = [];
      await traceConsumer.run({
        eachMessage: async ({ message }: EachMessagePayload) => {
          consumedMessages.push({
            headers: message.headers,
            value: message.value?.toString(),
          });
        },
      });

      // 2. Persist Outbox event in PG
      await outboxPrisma.client.outboxEvent.create({
        data: {
          id: eventId,
          aggregateId,
          aggregateType: 'Payment',
          eventType: envelope.eventType,
          payload: envelope as any,
          status: OutboxStatus.PENDING,
          requestId: randomUUID(),
          correlationId: envelope.correlationId,
          causationId: envelope.causationId,
          traceHeaders: {},
        },
      });

      // 3. Trigger Outbox Relay batch processing
      await outboxRelayService.processBatch();

      // 4. Assert header traceparent matches mockTraceParent
      await eventually(async () => {
        const myMsg = consumedMessages.find((msg) => {
          try {
            const parsed = JSON.parse(msg.value);
            return parsed.eventId === eventId;
          } catch {
            return false;
          }
        });
        expect(myMsg).toBeDefined();
        const headers = myMsg.headers;
        expect(headers).toBeDefined();
        
        const traceParentHeader = headers.traceparent?.toString();
        expect(traceParentHeader).toBe(mockTraceParent);
      });
    } finally {
      await traceConsumer.disconnect();
    }
  });
});
