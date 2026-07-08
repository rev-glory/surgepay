import * as fs from 'fs';
import * as path from 'path';

import * as dotenv from 'dotenv';
import Redis from 'ioredis';

// Load environment variables
const env = process.env.NODE_ENV || 'development';
const envFile = `.env.${env}`;
let envPath = path.resolve(process.cwd(), envFile);
if (!fs.existsSync(envPath)) {
  envPath = path.resolve(__dirname, '..', envFile);
}
dotenv.config({ path: envPath });

const GATEWAY_URL = 'http://127.0.0.1:3000/api/v1';
const API_KEY = 'sp_active_key_123';
const REDIS_URL = process.env.REDIS_URL || 'redis://:redis_secure_pass@127.0.0.1:6379';

async function runVerification() {
  console.log('=== Starting Rate Limiting Verification ===');
  console.log(`Gateway URL: ${GATEWAY_URL}`);
  console.log(`Redis URL: ${REDIS_URL}\n`);

  // Connect to Redis and clean up any pre-existing rate limit keys
  const redis = new Redis(REDIS_URL);
  const keys = await redis.keys('rate_limit:*');
  if (keys.length > 0) {
    console.log(`Cleaning up existing rate limit keys in Redis: ${keys.join(', ')}`);
    await redis.del(...keys);
  }

  // Scenario 1: Send 100 requests (Quota limit)
  console.log('Executing Scenario 1: Sending 100 requests concurrently...');
  const requests = [];
  for (let i = 1; i <= 100; i++) {
    requests.push(
      fetch(GATEWAY_URL, {
        headers: { 'x-api-key': API_KEY },
      })
    );
  }

  const responses = await Promise.all(requests);
  const successCount = responses.filter((r) => r.status === 200).length;
  console.log(`Result: ${successCount}/100 requests returned status 200 OK.`);
  if (successCount !== 100) {
    console.error('❌ SCENARIO 1 FAILED: Not all requests succeeded.');
    await redis.quit();
    process.exit(1);
  }
  console.log('✓ SCENARIO 1 PASSED\n');

  // Scenario 3: Verify response headers on a successful request
  console.log('Executing Scenario 3: Verifying response headers...');
  const sampleResponse = responses[0];
  const limitHeader = sampleResponse.headers.get('x-ratelimit-limit');
  const remainingHeader = sampleResponse.headers.get('x-ratelimit-remaining');
  const resetHeader = sampleResponse.headers.get('x-ratelimit-reset');

  console.log(`Headers - Limit: ${limitHeader}, Remaining: ${remainingHeader}, Reset: ${resetHeader}`);
  if (!limitHeader || !remainingHeader || !resetHeader) {
    console.error('❌ SCENARIO 3 FAILED: Missing rate limit headers.');
    await redis.quit();
    process.exit(1);
  }
  console.log('✓ SCENARIO 3 PASSED\n');

  // Scenario 2: Send 101st request (should fail with 429)
  console.log('Executing Scenario 2: Sending 101st request...');
  const rejectResponse = await fetch(GATEWAY_URL, {
    headers: { 'x-api-key': API_KEY },
  });

  console.log(`Status code: ${rejectResponse.status}`);
  if (rejectResponse.status !== 429) {
    console.error(`❌ SCENARIO 2 FAILED: Expected 429 but got ${rejectResponse.status}`);
    await redis.quit();
    process.exit(1);
  }

  const rejectBody = (await rejectResponse.json()) as any;
  console.log('Body:', JSON.stringify(rejectBody, null, 2));

  const expectedBody = {
    success: false,
    error: {
      code: 'RATE_LIMIT_EXCEEDED',
      message: 'Merchant rate limit exceeded.',
    },
  };

  if (
    rejectBody.success !== expectedBody.success ||
    rejectBody.error?.code !== expectedBody.error.code ||
    rejectBody.error?.message !== expectedBody.error.message
  ) {
    console.error('❌ SCENARIO 2 FAILED: Response body structure is incorrect.');
    await redis.quit();
    process.exit(1);
  }
  console.log('✓ SCENARIO 2 PASSED\n');

  // Scenario: Verify Retry-After header
  console.log('Executing Scenario: Verifying Retry-After header...');
  const retryAfterHeader = rejectResponse.headers.get('retry-after');
  console.log(`Retry-After Header: ${retryAfterHeader}`);
  if (!retryAfterHeader) {
    console.error('❌ RETRY-AFTER VERIFICATION FAILED: Retry-After header is missing.');
    await redis.quit();
    process.exit(1);
  }
  const retryAfterSeconds = parseInt(retryAfterHeader, 10);
  if (isNaN(retryAfterSeconds) || retryAfterSeconds <= 0) {
    console.error(`❌ RETRY-AFTER VERIFICATION FAILED: Retry-After value is invalid: ${retryAfterHeader}`);
    await redis.quit();
    process.exit(1);
  }
  console.log('✓ RETRY-AFTER VERIFICATION PASSED\n');

  // Scenario 4: Verify Redis contains key with TTL
  console.log('Executing Scenario 4: Verifying Redis key and TTL...');
  const redisKeys = await redis.keys('rate_limit:*');
  console.log(`Redis Keys found: ${redisKeys.join(', ')}`);
  if (redisKeys.length === 0) {
    console.error('❌ SCENARIO 4 FAILED: No rate limit key found in Redis.');
    await redis.quit();
    process.exit(1);
  }

  const ttl = await redis.ttl(redisKeys[0]);
  console.log(`Key TTL: ${ttl} seconds`);
  if (ttl <= 0 || ttl > 60) {
    console.error(`❌ SCENARIO 4 FAILED: Key TTL ${ttl} is invalid.`);
    await redis.quit();
    process.exit(1);
  }
  console.log('✓ SCENARIO 4 PASSED\n');

  // Cleanup
  await redis.quit();
  console.log('=== All verification scenarios completed successfully! ===');
}

runVerification().catch(async (err) => {
  console.error('Verification script crashed:', err);
  process.exit(1);
});
