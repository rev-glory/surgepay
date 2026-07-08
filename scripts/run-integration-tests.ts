import { execSync } from 'child_process';

import type { StartedTestContainer } from 'testcontainers';
import { GenericContainer } from 'testcontainers';

async function main() {
  console.log('======================================================');
  console.log('🚀 Starting SurgePay Integration Test Infrastructure...');
  console.log('======================================================');

  let redisContainer: StartedTestContainer | null = null;
  let postgresContainer: StartedTestContainer | null = null;

  try {
    // 1. Start Redis Container
    console.log('📦 Starting Redis container...');
    redisContainer = await new GenericContainer('redis:alpine')
      .withExposedPorts(6379)
      .start();
    const redisPort = redisContainer.getMappedPort(6379);
    const redisHost = redisContainer.getHost();
    const redisUrl = `redis://${redisHost}:${redisPort}`;
    console.log(`✓ Redis container ready at ${redisUrl}`);

    // 2. Start PostgreSQL Container
    console.log('📦 Starting PostgreSQL container...');
    postgresContainer = await new GenericContainer('postgres:alpine')
      .withExposedPorts(5432)
      .withEnvironment({
        POSTGRES_USER: 'surgepay_admin',
        POSTGRES_PASSWORD: 'surgepay_secure_pass',
        POSTGRES_DB: 'surgepay_test_db',
      })
      .start();
    const pgPort = postgresContainer.getMappedPort(5432);
    const pgHost = postgresContainer.getHost();
    const databaseUrl = `postgresql://surgepay_admin:surgepay_secure_pass@${pgHost}:${pgPort}/surgepay_test_db?sslmode=disable&schema=merchant`;
    console.log(`✓ PostgreSQL container ready at ${databaseUrl}`);

    // Set environment variables for the current process and spawned child processes
    process.env.DATABASE_URL = databaseUrl;
    process.env.REDIS_URL = redisUrl;
    process.env.NODE_ENV = 'test';
    process.env.REDIS_PASSWORD = '';

    // 3. Execute Prisma DB Push
    console.log('🔨 Running Prisma schema push to test database...');
    execSync('npx prisma db push --schema=apps/merchant-service/prisma/schema.prisma', {
      stdio: 'inherit',
      env: { ...process.env },
    });
    console.log('✓ Database schema synchronized successfully.');

    // 4. Run Jest Integration Tests
    console.log('🧪 Running integration test suite...');
    execSync('npx jest --config jest.integration.config.js --runInBand --forceExit', {
      stdio: 'inherit',
      env: { ...process.env },
    });
    console.log('✓ All integration tests completed successfully.');
  } catch (error) {
    console.error('❌ Integration test execution failed:', error);
    process.exitCode = 1;
  } finally {
    console.log('======================================================');
    console.log('🧹 Cleaning up integration test infrastructure...');
    console.log('======================================================');

    if (redisContainer) {
      try {
        await redisContainer.stop();
        console.log('✓ Redis container stopped.');
      } catch (err) {
        console.error('Failed to stop Redis container:', err);
      }
    }

    if (postgresContainer) {
      try {
        await postgresContainer.stop();
        console.log('✓ PostgreSQL container stopped.');
      } catch (err) {
        console.error('Failed to stop PostgreSQL container:', err);
      }
    }

    console.log('🏁 Infrastructure teardown complete.');
  }
}

main().catch((err) => {
  console.error('Fatal crash in integration test runner:', err);
  process.exit(1);
});
