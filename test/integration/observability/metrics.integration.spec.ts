import { randomUUID } from 'crypto';
import { Test, TestingModule } from '@nestjs/testing';
import { ConfigModule } from '@surgepay/config';
import { LoggerModule, MetricsService, KafkaEventProducer, CURRENT_EVENT_VERSION, MetricsController, Registry } from '@surgepay/common';
import { OrderModule } from '../../../apps/order-service/src/modules/order.module';
import { PrismaModule } from '../../../apps/order-service/src/prisma/prisma.module';
import { PrismaService } from '../../../apps/order-service/src/prisma/prisma.service';
import { OrderEventConsumer } from '../../../apps/order-service/src/services/order-event.consumer';
import { BaseEventEnvelope } from '@surgepay/events';

// Mock kafkajs
const mockCommitOffsets = jest.fn().mockResolvedValue(undefined);
const mockSend = jest.fn().mockResolvedValue([{ topicName: 'payments.dlq', partition: 0 }]);
jest.mock('kafkajs', () => {
  return {
    CompressionTypes: {
      None: 0,
      GZIP: 1,
      Snappy: 2,
      LZ4: 3,
      ZSTD: 4,
    },
    Kafka: jest.fn().mockImplementation(() => {
      return {
        producer: jest.fn().mockImplementation(() => {
          return {
            connect: jest.fn().mockResolvedValue(undefined),
            send: mockSend,
            disconnect: jest.fn().mockResolvedValue(undefined),
          };
        }),
        consumer: jest.fn().mockImplementation(() => {
          return {
            connect: jest.fn().mockResolvedValue(undefined),
            subscribe: jest.fn().mockResolvedValue(undefined),
            run: jest.fn().mockImplementation(async (options) => {
              (global as any).mockEachMessage = options.eachMessage;
            }),
            disconnect: jest.fn().mockResolvedValue(undefined),
            commitOffsets: mockCommitOffsets,
          };
        }),
      };
    }),
  };
});

describe('Prometheus Metrics Integration Tests', () => {
  let moduleFixture: TestingModule;
  let prismaService: PrismaService;
  let consumer: OrderEventConsumer;
  let metricsService: MetricsService;
  let customRegistry: Registry;
  let producer: KafkaEventProducer;
  let metricsController: MetricsController;

  beforeAll(async () => {
    customRegistry = new Registry();

    const originalUrl = process.env.DATABASE_URL;
    if (originalUrl) {
      const url = new URL(originalUrl);
      url.searchParams.delete('schema');
      process.env.DATABASE_URL = url.toString();
    }

    try {
      moduleFixture = await Test.createTestingModule({
        imports: [
          ConfigModule,
          LoggerModule,
          PrismaModule,
          OrderModule,
        ],
      })
      .overrideProvider('CUSTOM_REGISTRY')
      .useValue(customRegistry)
      .compile();
    } finally {
      process.env.DATABASE_URL = originalUrl;
    }

    prismaService = moduleFixture.get<PrismaService>(PrismaService);
    consumer = moduleFixture.get<OrderEventConsumer>(OrderEventConsumer);
    metricsService = moduleFixture.get<MetricsService>(MetricsService);
    producer = moduleFixture.get<KafkaEventProducer>(KafkaEventProducer);
    metricsController = moduleFixture.get<MetricsController>(MetricsController);

    await prismaService.client.$connect();
    await consumer.onModuleInit();
  });

  afterAll(async () => {
    await consumer.onModuleDestroy();
    await prismaService.client.$disconnect();
    await moduleFixture.close();
  });

  beforeEach(async () => {
    await prismaService.client.inboxEvent.deleteMany();
  });

  it('verifies registry isolation and CUSTOM_REGISTRY binding', () => {
    expect(metricsService.registry).toBe(customRegistry);
  });

  it('records producer publish metrics on success', async () => {
    const event: BaseEventEnvelope<any> = {
      eventId: randomUUID(),
      eventType: 'payment.test',
      correlationId: randomUUID(),
      causationId: randomUUID(),
      sagaId: randomUUID(),
      timestamp: new Date().toISOString(),
      version: CURRENT_EVENT_VERSION,
      payload: {},
    };

    await producer.publish('test-topic', 'test-key', event);

    const metricsStr = await metricsService.registry.metrics();
    expect(metricsStr).toContain('events_published_total');
    expect(metricsStr).toContain('publish_duration_ms');
  });

  it('records consumer consume attempt and handler success metrics', async () => {
    const eventId = randomUUID();
    const envelope: BaseEventEnvelope<any> = {
      eventId,
      eventType: 'order.created',
      correlationId: randomUUID(),
      causationId: randomUUID(),
      sagaId: randomUUID(),
      timestamp: new Date().toISOString(),
      version: CURRENT_EVENT_VERSION,
      payload: { value: 'data' },
    };

    jest.spyOn(consumer as any, 'handleEvent').mockResolvedValue(undefined);

    const handler = (global as any).mockEachMessage;
    await handler({
      topic: 'orders',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '10',
      },
    });

    const metricsStr = await metricsService.registry.metrics();
    expect(metricsStr).toContain('events_consumed_total');
    expect(metricsStr).toContain('inbox_received_events_total');
    expect(metricsStr).toContain('inbox_processed_events_total');
    expect(metricsStr).toContain('consumer_duration_ms');
  });

  it('records duplicate events skipped', async () => {
    const eventId = randomUUID();
    const envelope: BaseEventEnvelope<any> = {
      eventId,
      eventType: 'order.created',
      correlationId: randomUUID(),
      causationId: randomUUID(),
      sagaId: randomUUID(),
      timestamp: new Date().toISOString(),
      version: CURRENT_EVENT_VERSION,
      payload: { value: 'data' },
    };

    jest.spyOn(consumer as any, 'handleEvent').mockResolvedValue(undefined);

    const handler = (global as any).mockEachMessage;

    // First delivery (success)
    await handler({
      topic: 'orders',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '20',
      },
    });

    // Second delivery (duplicate)
    await handler({
      topic: 'orders',
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '21',
      },
    });

    const metricsStr = await metricsService.registry.metrics();
    expect(metricsStr).toContain('duplicate_events_total');
  });

  it('records consumer failures and retries', async () => {
    const eventId = randomUUID();
    const envelope: BaseEventEnvelope<any> = {
      eventId,
      eventType: 'order.created',
      correlationId: randomUUID(),
      causationId: randomUUID(),
      sagaId: randomUUID(),
      timestamp: new Date().toISOString(),
      version: CURRENT_EVENT_VERSION,
      payload: { value: 'data' },
    };

    jest.spyOn(consumer as any, 'handleEvent').mockRejectedValue(new Error('Handler failure'));

    const handler = (global as any).mockEachMessage;

    try {
      await handler({
        topic: 'orders',
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify(envelope)),
          offset: '30',
        },
      });
    } catch (e) {
      // Expected
    }

    const metricsStr = await metricsService.registry.metrics();
    expect(metricsStr).toContain('consumer_failures_total');
  });

  it('verifies GET /metrics HTTP controller behavior', async () => {
    const mockRes = {
      set: jest.fn(),
      end: jest.fn(),
    } as any;

    await metricsController.getMetrics(mockRes);

    expect(mockRes.set).toHaveBeenCalledWith('Content-Type', metricsService.registry.contentType);
    expect(mockRes.end).toHaveBeenCalled();
    const output = mockRes.end.mock.calls[0][0];
    expect(output).toContain('events_published_total');
  });
});
