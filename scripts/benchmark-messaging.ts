import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';

// Configure Keep-Alive and max sockets environment variables with sensible defaults
process.env.HTTP_KEEP_ALIVE = process.env.HTTP_KEEP_ALIVE ?? 'true';
process.env.HTTP_KEEP_ALIVE_MAX_SOCKETS = process.env.HTTP_KEEP_ALIVE_MAX_SOCKETS ?? '100';

// Setup environment and load correct variables
const env = process.env.NODE_ENV || 'test';
const envFile = `.env.${env}`;
let envPath = path.resolve(process.cwd(), envFile);
if (!fs.existsSync(envPath)) {
  envPath = path.resolve(__dirname, '..', envFile);
}
dotenv.config({ path: envPath });

import { setupE2EEnvironment, teardownE2EEnvironment } from '../test/helpers/test-setup';
import { KafkaEventProducer } from '../packages/common/src/messaging/producer';
import { ConfigService } from '@surgepay/config';
import { LoggerService } from '../packages/common/src/logger';
import { OutboxRelayService } from '../apps/outbox-relay/src/relay.service';
import { OutboxScheduler } from '../apps/outbox-relay/src/scheduler';
import { PrismaClient } from '../apps/outbox-relay/src/generated/client';

const numMessages = process.env.BENCHMARK_MESSAGES ? parseInt(process.env.BENCHMARK_MESSAGES, 10) : 500;
const batchSize = process.env.BENCHMARK_BATCH_SIZE ? parseInt(process.env.BENCHMARK_BATCH_SIZE, 10) : 50;

async function runBenchmark() {
  console.log('================================================================');
  console.log('🚀 SurgePay Messaging Pipeline Throughput Benchmark');
  console.log('================================================================');
  console.log(`• Total Database Relay Events: 5000`);
  console.log(`• Batch Size:                  ${batchSize}`);
  console.log('================================================================\n');

  console.log('📦 Spin up local isolated container environment...');
  const e2eEnv = await setupE2EEnvironment();
  
  if (!e2eEnv.gatewayApp || !e2eEnv.relayApp) {
    throw new Error('Nest apps failed to initialize in E2E environment.');
  }

  const config = e2eEnv.gatewayApp.get<ConfigService>(ConfigService);
  const logger = {
    setContext: () => {},
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
  } as unknown as LoggerService;

  // 1. Resolve Relay and stop the background Scheduler to prevent overlap/jitter
  console.log('⚙️  Stopping background Outbox scheduler...');
  const outboxRelayService = e2eEnv.relayApp.get<OutboxRelayService>(OutboxRelayService);
  const scheduler = e2eEnv.relayApp.get<OutboxScheduler>(OutboxScheduler);
  await scheduler.onApplicationShutdown();

  // 2. Instantiate Prisma client targeting the 'payment' schema
  console.log('🔌 Connecting to payment database schema...');
  const paymentDbUrl = new URL(process.env.DATABASE_URL || config.database.url);
  paymentDbUrl.searchParams.set('schema', 'payment');
  const paymentPrisma = new PrismaClient({
    datasources: {
      db: {
        url: paymentDbUrl.toString(),
      },
    },
  });
  await paymentPrisma.$connect();

  // 3. Clear database tables
  console.log('🧹 Wiping payment and outbox database tables...');
  await paymentPrisma.$executeRawUnsafe('TRUNCATE TABLE "payment"."Payment" CASCADE;');
  await paymentPrisma.$executeRawUnsafe('TRUNCATE TABLE "payment"."OutboxEvent" CASCADE;');

  // 4. Generate 5,000 pending Outbox events
  console.log('📋 Generating 5,000 pending outbox events...');
  const eventsData = [];
  const dummyEnvelope = {
    eventId: '',
    eventType: 'PaymentInitiated',
    correlationId: 'corr_bench',
    causationId: 'caus_bench',
    sagaId: 'saga_bench',
    requestId: 'req_bench',
    timestamp: new Date().toISOString(),
    version: 1,
    payload: { amount: 100 },
  };

  for (let i = 0; i < 5000; i++) {
    const eventId = crypto.randomUUID();
    const correlationId = crypto.randomUUID();
    const causationId = crypto.randomUUID();
    const aggregateId = crypto.randomUUID();

    const envelope = {
      ...dummyEnvelope,
      eventId,
      correlationId,
      causationId,
      sagaId: correlationId,
      requestId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
    };

    eventsData.push({
      id: eventId,
      aggregateId,
      aggregateType: 'Payment',
      eventType: 'PaymentInitiated',
      payload: envelope as any,
      status: 'PENDING' as any,
      requestId: envelope.requestId,
      correlationId,
      causationId,
      createdAt: new Date(),
      traceHeaders: {},
    });
  }

  // 5. Seed database using fast createMany
  console.log('💾 Seeding 5,000 rows into "payment"."OutboxEvent"...');
  const seedStartTime = performance.now();
  const chunkSize = 1000;
  for (let i = 0; i < eventsData.length; i += chunkSize) {
    const chunk = eventsData.slice(i, i + chunkSize);
    await paymentPrisma.outboxEvent.createMany({ data: chunk });
  }
  const seedDuration = performance.now() - seedStartTime;
  console.log(`✓ Seeded 5,000 database rows in ${(seedDuration / 1000).toFixed(3)} s.\n`);

  // 6. Drive the Outbox Relay batch processing cycle until all 5,000 are PUBLISHED
  console.log('⚡ Driving Outbox Relay pipeline processing loop...');
  const relayStartTime = performance.now();

  let publishedCount = 0;
  let cycles = 0;
  while (publishedCount < 5000) {
    await outboxRelayService.processBatch();
    publishedCount = await paymentPrisma.outboxEvent.count({
      where: { status: 'PUBLISHED' },
    });
    cycles++;
    console.log(`   [Cycle ${cycles}] ${publishedCount} / 5000 events PUBLISHED`);
  }

  const relayDuration = performance.now() - relayStartTime;
  const relayRps = (5000 / relayDuration) * 1000;

  console.log('\n================================================================');
  console.log('📊 Outbox Relay Performance Benchmark Results');
  console.log('================================================================');
  console.log(`• Total Events Processed:   5000`);
  console.log(`• Convergence Time:         ${(relayDuration / 1000).toFixed(3)} s`);
  console.log(`• Total Relay Cycles Run:   ${cycles}`);
  console.log(`• Relay Throughput:         ${relayRps.toFixed(2)} events/sec`);
  
  if (relayRps >= 1000) {
    console.log(`• Status:                   PASSED (Exceeds 1,000 events/sec target)`);
  } else {
    console.log(`• Status:                   FAILED (Under 1,000 events/sec target)`);
  }
  console.log('================================================================\n');

  // 7. Additional benchmark: Single vs Batch producer publishing comparison
  console.log('⏱️  Starting producer publishing comparison (500 events)...');
  const producer = new KafkaEventProducer(config, logger);
  await producer.onModuleInit();
  const testTopic = 'payments.initiated';

  const singleStartTime = performance.now();
  for (let i = 0; i < numMessages; i++) {
    const envelope = { ...dummyEnvelope, eventId: crypto.randomUUID() };
    await producer.publish(testTopic, `key_${i}`, envelope);
  }
  const singleDuration = performance.now() - singleStartTime;
  const singleRps = (numMessages / singleDuration) * 1000;
  console.log(`   Single Producer:  ${singleRps.toFixed(2)} msg/sec`);

  const batchStartTime = performance.now();
  for (let i = 0; i < numMessages; i += batchSize) {
    const items = [];
    const limit = Math.min(batchSize, numMessages - i);
    for (let k = 0; k < limit; k++) {
      items.push({
        topic: testTopic,
        key: `key_${i + k}`,
        event: { ...dummyEnvelope, eventId: crypto.randomUUID() },
      });
    }
    await producer.publishBatch(items);
  }
  const batchDuration = performance.now() - batchStartTime;
  const batchRps = (numMessages / batchDuration) * 1000;
  console.log(`   Batch Producer:   ${batchRps.toFixed(2)} msg/sec`);
  console.log(`   Speedup Factor:   ${(batchRps / singleRps).toFixed(2)}x\n`);

  console.log('🧹 Shutting down environment...');
  await producer.onModuleDestroy();
  await paymentPrisma.$disconnect();
  await teardownE2EEnvironment();
  console.log('🏁 Benchmark complete. Clean shutdown.');
  process.exit(0);
}

runBenchmark().catch(async (error) => {
  console.error('❌ Benchmark execution failed with fatal error:', error);
  try {
    await teardownE2EEnvironment();
  } catch (_e) {}
  process.exit(1);
});
