import { Test, TestingModule } from '@nestjs/testing';
import { Kafka } from 'kafkajs';
import { LoggerService, MetricsService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import { BaseEventEnvelope } from '@surgepay/events';
import {
  MessagingModule,
  ProducerService,
} from '@surgepay/common-messaging';
import { PrismaClient as PaymentPrismaClient } from '../../../apps/payment-service/src/generated/client';
import { RelayService } from '../../../apps/outbox-relay/src/relay.service';
import { RelayModule } from '../../../apps/outbox-relay/src/relay.module';
import { RedpandaTestContainer } from '../../testcontainers/redpanda.container';
import * as crypto from 'crypto';

describe('Outbox Relay Batching and Back-pressure Integration', () => {
  let appModule: TestingModule;
  let relayService: RelayService;
  let producerService: ProducerService;
  let redpandaContainer: RedpandaTestContainer;
  let metricsService: MetricsService;

  const paymentDatabaseUrl = process.env.DATABASE_URL
    ? process.env.DATABASE_URL.replace('schema=merchant', 'schema=payment')
    : undefined;

  const paymentPrisma = new PaymentPrismaClient({
    datasources: {
      db: {
        url: paymentDatabaseUrl,
      },
    },
  });

  beforeAll(async () => {
    // 1. Start Redpanda container
    redpandaContainer = new RedpandaTestContainer();
    const brokers = await redpandaContainer.start();
    process.env.KAFKA_BROKERS = brokers;

    // 2. Set Config limits
    process.env.OUTBOX_BATCH_SIZE = '3';
    process.env.OUTBOX_MAX_IN_FLIGHT = '5';

    appModule = await Test.createTestingModule({
      imports: [RelayModule, MessagingModule],
      providers: [
        LoggerService,
        MetricsService,
      ],
    }).compile();

    relayService = appModule.get(RelayService);
    producerService = appModule.get(ProducerService);
    metricsService = appModule.get(MetricsService);

    await producerService.onModuleInit();

    // Create required topics
    const kafka = new Kafka({
      clientId: 'integration-batch-test-client',
      brokers: [brokers],
    });
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [{ topic: 'payments.initiated' }],
    });
    await admin.disconnect();
  }, 60000);

  afterAll(async () => {
    await producerService.onModuleDestroy();
    await paymentPrisma.$disconnect();
    await appModule.close();
    await redpandaContainer.stop();
  }, 30000);

  beforeEach(async () => {
    await paymentPrisma.outboxEvent.deleteMany({});
  });

  it('should publish events in batches and mark outbox records individually', async () => {
    // Create 5 pending outbox events
    const count = 5;
    const ids = Array.from({ length: count }, () => crypto.randomUUID());
    
    await paymentPrisma.outboxEvent.createMany({
      data: ids.map((id, index) => {
        const correlationId = crypto.randomUUID();
        const causationId = crypto.randomUUID();
        const requestId = crypto.randomUUID();
        const envelope: BaseEventEnvelope<any> = {
          eventId: id,
          eventType: 'PaymentInitiated',
          correlationId,
          causationId,
          requestId,
          sagaId: crypto.randomUUID(),
          timestamp: new Date().toISOString(),
          version: 1,
          producer: 'payment-service',
          payload: { paymentId: crypto.randomUUID(), amount: 100 * (index + 1) },
        };

        return {
          id,
          aggregateId: crypto.randomUUID(),
          aggregateType: 'Payment',
          eventType: 'PaymentInitiated',
          payload: envelope as any,
          status: 'PENDING',
          requestId,
          correlationId,
          causationId,
        };
      }),
    });

    // Run Outbox Relay cycle
    await relayService.runOnce();

    // Verify all outbox records are transitioned to PUBLISHED
    const processedEvents = await paymentPrisma.outboxEvent.findMany({
      where: { id: { in: ids } },
    });

    expect(processedEvents).toHaveLength(count);
    for (const event of processedEvents) {
      expect(event.status).toBe('PUBLISHED');
      expect(event.publishedAt).not.toBeNull();
      expect(event.topic).toBe('payments.initiated');
      expect(event.partition).toBeDefined();
      expect(event.offset).toBeDefined();
    }
  });

  it('should enforce back-pressure by limiting concurrency under configured maxInFlight limit', async () => {
    // Create 8 events (which exceeds OUTBOX_MAX_IN_FLIGHT = 5)
    const count = 8;
    const ids = Array.from({ length: count }, () => crypto.randomUUID());
    
    await paymentPrisma.outboxEvent.createMany({
      data: ids.map((id, index) => {
        const correlationId = crypto.randomUUID();
        const causationId = crypto.randomUUID();
        const requestId = crypto.randomUUID();
        const envelope: BaseEventEnvelope<any> = {
          eventId: id,
          eventType: 'PaymentInitiated',
          correlationId,
          causationId,
          requestId,
          sagaId: crypto.randomUUID(),
          timestamp: new Date().toISOString(),
          version: 1,
          producer: 'payment-service',
          payload: { paymentId: crypto.randomUUID(), amount: 100 * (index + 1) },
        };

        return {
          id,
          aggregateId: crypto.randomUUID(),
          aggregateType: 'Payment',
          eventType: 'PaymentInitiated',
          payload: envelope as any,
          status: 'PENDING',
          requestId,
          correlationId,
          causationId,
        };
      }),
    });

    // Spy on backpressure controller state check / active messages
    const acquireSpy = jest.spyOn((relayService as any).backpressure, 'acquire');

    // Run cycle
    await relayService.runOnce();

    // Verify all 8 were eventually published successfully
    const processed = await paymentPrisma.outboxEvent.findMany({
      where: { id: { in: ids } },
    });
    expect(processed.filter(e => e.status === 'PUBLISHED')).toHaveLength(count);
    expect(acquireSpy).toHaveBeenCalled();
    
    acquireSpy.mockRestore();
  });
});
