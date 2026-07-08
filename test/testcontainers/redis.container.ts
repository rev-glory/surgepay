import { GenericContainer, type StartedTestContainer } from 'testcontainers';

export class RedisTestContainer {
  private container: StartedTestContainer | null = null;

  async start(): Promise<string> {
    this.container = await new GenericContainer('redis:alpine')
      .withExposedPorts({ container: 6379, host: 26379 })
      .withStartupTimeout(60000)
      .start();

    const port = 26379;
    const host = this.container.getHost();
    return `redis://${host}:${port}`;
  }

  async stop(): Promise<void> {
    if (this.container) {
      await this.container.stop();
      this.container = null;
    }
  }

  async restart(): Promise<void> {
    if (this.container) {
      await this.container.restart();
    }
  }

  getContainer(): StartedTestContainer | null {
    return this.container;
  }
}
