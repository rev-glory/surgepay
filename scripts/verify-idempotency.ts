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
  console.log('=== Starting Idempotency Verification ===');
  console.log(`Gateway URL: ${GATEWAY_URL}`);
  console.log(`Redis URL: ${REDIS_URL}\n`);

  const redis = new Redis(REDIS_URL);
  
  // Clean up any existing idempotency keys
  const keys = await redis.keys('idem:*');
  if (keys.length > 0) {
    console.log(`Cleaning up existing idempotency keys in Redis: ${keys.join(', ')}`);
    await redis.del(...keys);
  }

  // -------------------------------------------------------------
  // Scenario 1: First request (MISS -> IN_PROGRESS -> COMPLETED)
  // -------------------------------------------------------------
  console.log('Executing Scenario 1: First request (MISS)...');
  const key1 = `idem_test_key_${Math.random().toString(36).substring(2, 9)}`;
  const body1 = { amount: 100, currency: 'USD' };

  const res1 = await fetch(`${GATEWAY_URL}/test-idempotency`, {
    method: 'POST',
    headers: {
      'x-api-key': API_KEY,
      'idempotency-key': key1,
      'content-type': 'application/json',
    },
    body: JSON.stringify(body1),
  });

  console.log(`Status code: ${res1.status}`);
  const replayedHeader1 = res1.headers.get('idempotency-replayed');
  console.log(`Idempotency-Replayed Header: ${replayedHeader1}`);
  const data1 = await res1.json() as any;
  console.log('Body:', JSON.stringify(data1));

  if (res1.status !== 202) {
    console.error(`❌ SCENARIO 1 FAILED: Expected 202 Accepted but got ${res1.status}`);
    process.exit(1);
  }
  if (replayedHeader1) {
    console.error(`❌ SCENARIO 1 FAILED: Expected Idempotency-Replayed to be absent but got ${replayedHeader1}`);
    process.exit(1);
  }
  console.log('✓ SCENARIO 1 PASSED\n');

  // Verify key state in Redis
  const redisKeys = await redis.keys(`idem:*:${key1}`);
  if (redisKeys.length === 0) {
    console.error('❌ REDIS STATE VERIFICATION FAILED: Key not found in Redis');
    process.exit(1);
  }
  const storedValue = await redis.get(redisKeys[0]);
  if (!storedValue) {
    console.error('❌ REDIS STATE VERIFICATION FAILED: Key value is empty in Redis');
    process.exit(1);
  }
  const parsedRecord = JSON.parse(storedValue);
  console.log('Redis Stored Record:', JSON.stringify(parsedRecord, null, 2));
  if (parsedRecord.status !== 'COMPLETED' || parsedRecord.statusCode !== 202) {
    console.error(`❌ REDIS STATE VERIFICATION FAILED: Expected COMPLETED (202) but got ${parsedRecord.status}`);
    process.exit(1);
  }
  console.log('✓ REDIS STATE VERIFICATION PASSED\n');

  // -------------------------------------------------------------
  // Scenario 2: Duplicate request after completion (Replay response)
  // -------------------------------------------------------------
  console.log('Executing Scenario 2: Duplicate request after completion (Replay)...');
  const res2 = await fetch(`${GATEWAY_URL}/test-idempotency`, {
    method: 'POST',
    headers: {
      'x-api-key': API_KEY,
      'idempotency-key': key1,
      'content-type': 'application/json',
    },
    body: JSON.stringify(body1),
  });

  console.log(`Status code: ${res2.status}`);
  const replayedHeader2 = res2.headers.get('idempotency-replayed');
  console.log(`Idempotency-Replayed Header: ${replayedHeader2}`);
  const data2 = await res2.json() as any;
  console.log('Body:', JSON.stringify(data2));

  if (res2.status !== 202) {
    console.error(`❌ SCENARIO 2 FAILED: Expected 202 but got ${res2.status}`);
    process.exit(1);
  }
  if (replayedHeader2 !== 'true') {
    console.error(`❌ SCENARIO 2 FAILED: Expected Idempotency-Replayed to be 'true' but got ${replayedHeader2}`);
    process.exit(1);
  }
  if (JSON.stringify(data2.data) !== JSON.stringify(data1.data)) {
    console.error('❌ SCENARIO 2 FAILED: Replayed payload differs from original');
    process.exit(1);
  }
  console.log('✓ SCENARIO 2 PASSED\n');

  // -------------------------------------------------------------
  // Scenario 3: Mismatched payload with same key (422 Unprocessable)
  // -------------------------------------------------------------
  console.log('Executing Scenario 3: Mismatched payload with same key...');
  const bodyDifferent = { amount: 200, currency: 'USD' }; // changed amount from 100 to 200
  const res3 = await fetch(`${GATEWAY_URL}/test-idempotency`, {
    method: 'POST',
    headers: {
      'x-api-key': API_KEY,
      'idempotency-key': key1,
      'content-type': 'application/json',
    },
    body: JSON.stringify(bodyDifferent),
  });

  console.log(`Status code: ${res3.status}`);
  const data3 = await res3.json() as any;
  console.log('Body:', JSON.stringify(data3));

  if (res3.status !== 422) {
    console.error(`❌ SCENARIO 3 FAILED: Expected 422 but got ${res3.status}`);
    process.exit(1);
  }
  if (data3.error?.code !== 'IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_REQUEST') {
    console.error(`❌ SCENARIO 3 FAILED: Expected IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_REQUEST but got ${JSON.stringify(data3.error)}`);
    process.exit(1);
  }
  console.log('✓ SCENARIO 3 PASSED\n');

  // -------------------------------------------------------------
  // Scenario 4: Concurrent requests locking (409 Conflict)
  // -------------------------------------------------------------
  console.log('Executing Scenario 4: Concurrent requests locking (409 Conflict)...');
  const key2 = `idem_test_key_${Math.random().toString(36).substring(2, 9)}`;
  
  // First request runs with a 1500ms delay in the test handler
  const reqA = fetch(`${GATEWAY_URL}/test-idempotency`, {
    method: 'POST',
    headers: {
      'x-api-key': API_KEY,
      'idempotency-key': key2,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ amount: 150, delay: 1500 }),
  });

  // Wait 300ms to ensure the first request has acquired the lock
  await new Promise((resolve) => setTimeout(resolve, 300));

  // Second request triggers while first is IN_PROGRESS
  const reqB = fetch(`${GATEWAY_URL}/test-idempotency`, {
    method: 'POST',
    headers: {
      'x-api-key': API_KEY,
      'idempotency-key': key2,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ amount: 150, delay: 1500 }),
  });

  const [resA, resB] = await Promise.all([reqA, reqB]);

  console.log(`Request A status code: ${resA.status}`);
  console.log(`Request B status code: ${resB.status}`);
  const dataB = await resB.json() as any;
  console.log('Request B Body:', JSON.stringify(dataB));

  if (resB.status !== 409) {
    console.error(`❌ SCENARIO 4 FAILED: Expected Request B to return 409 Conflict, got ${resB.status}`);
    process.exit(1);
  }
  if (dataB.error?.code !== 'REQUEST_ALREADY_IN_PROGRESS') {
    console.error(`❌ SCENARIO 4 FAILED: Expected REQUEST_ALREADY_IN_PROGRESS but got ${JSON.stringify(dataB.error)}`);
    process.exit(1);
  }
  if (resA.status !== 202) {
    console.error(`❌ SCENARIO 4 FAILED: Expected Request A to succeed with 202 Accepted, got ${resA.status}`);
    process.exit(1);
  }
  console.log('✓ SCENARIO 4 PASSED\n');

  // -------------------------------------------------------------
  // Scenario 5: Downstream failure lock cleanup
  // -------------------------------------------------------------
  console.log('Executing Scenario 5: Downstream failure lock cleanup...');
  const key3 = `idem_test_key_${Math.random().toString(36).substring(2, 9)}`;

  // This request will throw a 500 error in the test handler
  const resFail = await fetch(`${GATEWAY_URL}/test-idempotency`, {
    method: 'POST',
    headers: {
      'x-api-key': API_KEY,
      'idempotency-key': key3,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ amount: 50, fail: true }),
  });

  console.log(`Failed Request status code: ${resFail.status}`);
  if (resFail.status !== 500) {
    console.error(`❌ SCENARIO 5 FAILED: Expected 500 but got ${resFail.status}`);
    process.exit(1);
  }

  // Key should have been deleted on cleanup
  const redisKeysFor3 = await redis.keys(`idem:*:${key3}`);
  const existsInRedis = redisKeysFor3.length;
  console.log(`Key exists in Redis after failure: ${existsInRedis === 1}`);
  if (existsInRedis !== 0) {
    console.error('❌ SCENARIO 5 FAILED: Key was not deleted from Redis after downstream error');
    process.exit(1);
  }

  // Subsequent request with the same key should succeed as a new request
  console.log('Sending retry request for cleaned-up key...');
  const resRetry = await fetch(`${GATEWAY_URL}/test-idempotency`, {
    method: 'POST',
    headers: {
      'x-api-key': API_KEY,
      'idempotency-key': key3,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ amount: 50 }),
  });

  console.log(`Retry Request status code: ${resRetry.status}`);
  const dataRetry = await resRetry.json() as any;
  console.log('Retry Body:', JSON.stringify(dataRetry));

  if (resRetry.status !== 202) {
    console.error(`❌ SCENARIO 5 FAILED: Expected retry request to succeed with 202, got ${resRetry.status}`);
    process.exit(1);
  }
  console.log('✓ SCENARIO 5 PASSED\n');

  await redis.quit();
  console.log('=== All Idempotency Verification Scenarios Completed Successfully! ===');
}

runVerification().catch(async (err) => {
  console.error('Verification script crashed:', err);
  process.exit(1);
});
