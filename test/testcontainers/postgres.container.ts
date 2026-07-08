import type { StartedTestContainer } from 'testcontainers';
import { GenericContainer } from 'testcontainers';

export class PostgresTestContainer {
  private container: StartedTestContainer | null = null;

  async start(): Promise<string> {
    this.container = await new GenericContainer('postgres:alpine')
      .withExposedPorts(5432)
      .withEnvironment({
        POSTGRES_USER: 'surgepay_admin',
        POSTGRES_PASSWORD: 'surgepay_secure_pass',
        POSTGRES_DB: 'surgepay_test_db',
      })
      .start();

    const pgPort = this.container.getMappedPort(5432);
    const pgHost = this.container.getHost();
    return `postgresql://surgepay_admin:surgepay_secure_pass@${pgHost}:${pgPort}/surgepay_test_db?sslmode=disable&schema=merchant`;
  }

  async stop(): Promise<void> {
    if (this.container) {
      await this.container.stop();
      this.container = null;
    }
  }

  getContainer(): StartedTestContainer | null {
    return this.container;
  }
}
