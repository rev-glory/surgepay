import { execSync } from 'child_process';
import { createServer } from 'net';

import type { StartedTestContainer } from 'testcontainers';
import { GenericContainer } from 'testcontainers';

async function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.listen(0, () => {
      const port = (server.address() as any).port;
      server.close(() => resolve(port));
    });
    server.on('error', reject);
  });
}

async function main() {
  console.log('======================================================');
  console.log('🚀 Starting SurgePay Integration Test Infrastructure...');
  console.log('======================================================');

  let redisContainer: StartedTestContainer | null = null;
  let postgresContainer: StartedTestContainer | null = null;
  let redpandaContainer: StartedTestContainer | null = null;

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
    const baseDatabaseUrl = `postgresql://surgepay_admin:surgepay_secure_pass@${pgHost}:${pgPort}/surgepay_test_db?sslmode=disable`;
    const databaseUrl = `${baseDatabaseUrl}&schema=merchant`;
    console.log(`✓ PostgreSQL container ready at ${databaseUrl}`);

    // 3. Start Redpanda Container
    console.log('📦 Starting Redpanda container...');
    const redpandaPort = await getFreePort();
    console.log(`Resolved free host port for Redpanda: ${redpandaPort}`);

    redpandaContainer = await new GenericContainer('docker.redpanda.com/redpandadata/redpanda:v23.3.10')
      .withExposedPorts({ container: 29092, host: redpandaPort })
      .withCommand([
        'redpanda',
        'start',
        '--smp', '1',
        '--memory', '1G',
        '--overprovisioned',
        '--node-id', '0',
        '--kafka-addr', 'internal://0.0.0.0:9092,external://0.0.0.0:29092',
        '--advertise-kafka-addr', `internal://127.0.0.1:9092,external://127.0.0.1:${redpandaPort}`,
      ])
      .withStartupTimeout(90000)
      .start();

    const redpandaHost = redpandaContainer.getHost();
    const kafkaBrokers = `${redpandaHost}:${redpandaPort}`;
    console.log(`✓ Redpanda container ready at ${kafkaBrokers}`);

    // Set environment variables for the current process and spawned child processes
    process.env.DATABASE_URL = databaseUrl;
    process.env.REDIS_URL = redisUrl;
    process.env.KAFKA_BROKERS = kafkaBrokers;
    process.env.NODE_ENV = 'test';
    process.env.REDIS_PASSWORD = '';
    process.env.REDPANDA_CONTAINER_ID = redpandaContainer.getId();

    // 4. Bootstrap Kafka Topics
    const topics = [
      'payments.initiated',
      'payments.completed',
      'payments.failed',
      'payments.flagged',
      'payments.dlq',
      'ledger.commands',
      'ledger.events',
      'risk.commands',
      'risk.events',
      'balance.commands',
      'balance.events',
      'notification.commands',
      'notification.events'
    ];

    console.log('🔨 Bootstrapping Kafka topics in Redpanda container...');
    for (const topic of topics) {
      const result = await redpandaContainer.exec([
        'rpk',
        'topic',
        'create',
        topic,
        '--brokers',
        '127.0.0.1:9092',
      ]);
      if (result.exitCode !== 0) {
        throw new Error(`Failed to create topic ${topic}: ${result.output}`);
      }
    }
    console.log('✓ Kafka topics bootstrapped successfully.');

    // 5. Execute Prisma DB Push
    console.log('🔨 Running Prisma schema push to test database...');
    execSync('npx prisma db push --schema=apps/merchant-service/prisma/schema.prisma --skip-generate', {
      stdio: 'inherit',
      env: {
        ...process.env,
        DATABASE_URL: `${baseDatabaseUrl}&schema=merchant`,
      },
    });
    execSync('npx prisma db push --schema=apps/payment-service/prisma/schema.prisma --skip-generate', {
      stdio: 'inherit',
      env: {
        ...process.env,
        DATABASE_URL: `${baseDatabaseUrl}&schema=payment`,
      },
    });
    execSync('npx prisma db push --schema=apps/order-service/src/prisma/order.prisma --skip-generate', {
      stdio: 'inherit',
      env: {
        ...process.env,
        DATABASE_URL: `${baseDatabaseUrl}&schema=order`,
      },
    });
    execSync('npx prisma db push --schema=apps/ledger-service/prisma/schema.prisma --skip-generate', {
      stdio: 'inherit',
      env: {
        ...process.env,
        DATABASE_URL: `${baseDatabaseUrl}&schema=ledger`,
      },
    });
    console.log('✓ Database schema synchronized successfully.');

    // 6. Run Jest Integration Tests
    console.log('🧪 Running integration test suite...');
    execSync('npx jest --config jest.integration.config.js --maxWorkers=1 --detectOpenHandles', {
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

    if (redpandaContainer) {
      try {
        await redpandaContainer.stop();
        console.log('✓ Redpanda container stopped.');
      } catch (err) {
        console.error('Failed to stop Redpanda container:', err);
      }
    }

    console.log('🏁 Infrastructure teardown complete.');
  }
}

main().catch((err) => {
  console.error('Fatal crash in integration test runner:', err);
  process.exit(1);
});
