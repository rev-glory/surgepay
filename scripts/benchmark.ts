import * as crypto from 'crypto';
import * as fs from 'fs';
import { AddressInfo } from 'net';
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
process.env.RATE_LIMIT_DEFAULT_LIMIT = '100000';

import { setupE2EEnvironment, teardownE2EEnvironment } from '../test/helpers/test-setup';
import { createTestMerchant, createTestOrder, clearDatabase } from '../test/helpers/db-helper';
import { clearRedis } from '../test/helpers/redis-helper';
import { MERCHANT_FIXTURES } from '../test/fixtures/merchants.fixture';

// Configuration parameters
const numRequests = process.env.BENCHMARK_REQUESTS ? parseInt(process.env.BENCHMARK_REQUESTS, 10) : 1000;
const concurrency = process.env.BENCHMARK_CONCURRENCY ? parseInt(process.env.BENCHMARK_CONCURRENCY, 10) : 20;
const warmupIterations = process.env.BENCHMARK_WARMUP ? parseInt(process.env.BENCHMARK_WARMUP, 10) : 50;

async function runBenchmark() {
  console.log('================================================================');
  console.log('🚀 SurgePay Synchronous Request Pipeline Benchmark Runner');
  console.log('================================================================');
  console.log(`• Total Requests:    ${numRequests}`);
  console.log(`• Concurrency:       ${concurrency}`);
  console.log(`• Warmup Iterations: ${warmupIterations}`);
  console.log(`• HTTP Keep-Alive:   ${process.env.HTTP_KEEP_ALIVE}`);
  console.log(`• Max Sockets:       ${process.env.HTTP_KEEP_ALIVE_MAX_SOCKETS}`);
  console.log('================================================================\n');

  console.log('📦 Spin up local isolated container environment...');
  const e2eEnv = await setupE2EEnvironment();
  const gatewayPort = (e2eEnv.gatewayApp.getHttpServer().address() as AddressInfo).port;
  const gatewayUrl = `http://127.0.0.1:${gatewayPort}`;
  console.log(`📡 Gateway App listening on port: ${gatewayPort}`);
  console.log(`📡 Benchmark targeting Gateway URL: ${gatewayUrl}`);
  const apiKey = MERCHANT_FIXTURES.active.apiKey;

  console.log('🧹 Wipe databases and Redis state...');
  await clearDatabase();
  await clearRedis();

  console.log('👤 Seeding active merchant account...');
  const merchant = await createTestMerchant(MERCHANT_FIXTURES.active);
  const merchantId = merchant.merchantId;

  console.log(`📋 Generating ${numRequests + warmupIterations} test orders...`);
  const orders: { orderId: string; idempotencyKey: string }[] = [];
  for (let i = 0; i < numRequests + warmupIterations; i++) {
    const orderId = crypto.randomUUID();
    const idempotencyKey = `idem_bench_${i}_${Date.now()}`;
    orders.push({ orderId, idempotencyKey });
  }

  // Seed DB with orders in batches to run faster
  const batchSize = 100;
  console.log('⚙️  Writing seeded orders to the database...');
  for (let i = 0; i < orders.length; i += batchSize) {
    const batch = orders.slice(i, i + batchSize);
    await Promise.all(
      batch.map(o => createTestOrder({
        merchantId,
        reference: o.orderId,
        amount: 1000,
        currency: 'USD',
        status: 'CREATED',
      }))
    );
  }
  console.log('✓ Database seeding complete.\n');

  // Warmup phase
  console.log(`🔥 Starting warmup phase (${warmupIterations} iterations)...`);
  for (let i = 0; i < warmupIterations; i++) {
    const order = orders[i];

    if (!order) {
      throw new Error(`Warmup order not found at index ${i}`);
    }

    const { orderId, idempotencyKey } = order;
    const response = await fetch(`${gatewayUrl}/api/v1/payments`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'idempotency-key': idempotencyKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        idempotencyKey,
        amount: 1000,
        currency: 'USD',
        merchantId,
        orderId,
        paymentMethod: 'card',
      }),
    });

    if (response.status !== 202) {
      const body = await response.text();

      throw new Error(
        `Warmup request failed: HTTP ${response.status} ${response.statusText}\n${body}`,
      );
    }
  }
  console.log('✓ Warmup phase complete. JIT compiler and sockets are warmed.\n');

  // Execution phase
  console.log(`⚡ Executing ${numRequests} requests with concurrency=${concurrency}...`);
  const latencies: number[] = [];
  let successfulRequests = 0;
  let failedRequests = 0;
  
  const activeBenchmarkOrders = orders.slice(warmupIterations);
  const queue = Array.from({ length: numRequests }, (_, i) => i);
  const startTime = performance.now();

  const runRequest = async (index: number) => {
    const order = activeBenchmarkOrders[index];

    if (!order) {
      throw new Error(`Benchmark order not found at index ${index}`);
    }

    const { orderId, idempotencyKey } = order;
    const reqStartTime = performance.now();

    try {
      const response = await fetch(`${gatewayUrl}/api/v1/payments`, {
        method: 'POST',
        headers: {
          'x-api-key': apiKey,
          'idempotency-key': idempotencyKey,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          idempotencyKey,
          amount: 1000,
          currency: 'USD',
          merchantId,
          orderId,
          paymentMethod: 'card',
        }),
      });

      const duration = performance.now() - reqStartTime;
      latencies.push(duration);

      if (response.status === 202) {
        successfulRequests++;
      } else {
        failedRequests++;

        if (failedRequests <= 10) {
          const body = await response.text();

          console.error('❌ Request failed', {
            status: response.status,
            statusText: response.statusText,
            body,
            orderId,
            idempotencyKey,
          });
        }
      }
    } catch (error) {
      const duration = performance.now() - reqStartTime;
      latencies.push(duration);
      failedRequests++;

      if (failedRequests <= 10) {
        console.error('❌ Request threw an error', {
          error,
          orderId,
          idempotencyKey,
        });
      }
    }
  };

  const worker = async () => {
    while (queue.length > 0) {
      const nextIndex = queue.shift();
      if (nextIndex === undefined) break;
      await runRequest(nextIndex);
    }
  };

  // Launch parallel workers
  const workers: Promise<void>[] = [];
  for (let i = 0; i < concurrency; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);
  const totalDurationMs = performance.now() - startTime;

  // Process and output results
  latencies.sort((a, b) => a - b);
  const totalDurationSeconds = totalDurationMs / 1000;
  const rps = (numRequests / totalDurationMs) * 1000;
  
  const getPercentile = (p: number): number => {
    if (latencies.length === 0) {
      throw new Error('Cannot calculate percentile: no latency samples recorded');
    }

    const index = Math.ceil((p / 100) * latencies.length) - 1;
    const value = latencies[Math.max(0, index)];

    if (value === undefined) {
      throw new Error(`Latency sample not found for percentile ${p}`);
    }

    return value;
  };

  if (latencies.length === 0) {
    throw new Error('Benchmark completed without recording latency samples');
  }

  const min = latencies[0];
  const max = latencies[latencies.length - 1];

  if (min === undefined || max === undefined) {
    throw new Error('Unable to calculate latency bounds');
  }

  const sum = latencies.reduce((a, b) => a + b, 0);
  const avg = sum / latencies.length;
  const median = getPercentile(50);
  const p95 = getPercentile(95);
  const p99 = getPercentile(99);

  console.log('================================================================');
  console.log('📊 Benchmark Results Summary');
  console.log('================================================================');
  console.log(`• Elapsed Time:          ${totalDurationSeconds.toFixed(3)} s`);
  console.log(`• Throughput:            ${rps.toFixed(2)} req/sec`);
  console.log(`• Successful Requests:   ${successfulRequests}`);
  console.log(`• Failed Requests:       ${failedRequests}`);
  console.log('----------------------------------------------------------------');
  console.log('⏱️  Latency Statistics');
  console.log('----------------------------------------------------------------');
  console.log(`• Min Latency:           ${min.toFixed(2)} ms`);
  console.log(`• Max Latency:           ${max.toFixed(2)} ms`);
  console.log(`• Average Latency:       ${avg.toFixed(2)} ms`);
  console.log(`• Median Latency:        ${median.toFixed(2)} ms`);
  console.log(`• P95 Latency:           ${p95.toFixed(2)} ms`);
  console.log(`• P99 Latency:           ${p99.toFixed(2)} ms`);
  console.log('================================================================\n');

  console.log('🧹 Shutting down environment containers...');
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
