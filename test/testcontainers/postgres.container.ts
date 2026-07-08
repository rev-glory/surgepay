import { GenericContainer, type StartedTestContainer } from 'testcontainers';

export class PostgresTestContainer {
  private container: StartedTestContainer | null = null;

  async start(): Promise<string> {
    this.container = await new GenericContainer('postgres:alpine')
      .withExposedPorts({ container: 5432, host: 25432 })
      .withEnvironment({
        POSTGRES_USER: 'surgepay_admin',
        POSTGRES_PASSWORD: 'surgepay_secure_pass',
        POSTGRES_DB: 'surgepay_test_db',
      })
      .withStartupTimeout(60000)
      .start();

    const pgPort = 25432;
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
