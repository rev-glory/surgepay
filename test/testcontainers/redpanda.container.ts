import { createServer } from 'net';
import { GenericContainer, type StartedTestContainer } from 'testcontainers';

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

export class RedpandaTestContainer {
  private container: StartedTestContainer | null = null;
  private port!: number;

  async start(): Promise<string> {
    this.port = await getFreePort();

    this.container = await new GenericContainer('docker.redpanda.com/redpandadata/redpanda:v23.3.10')
      .withExposedPorts({ container: 29092, host: this.port })
      .withCommand([
        'redpanda',
        'start',
        '--smp', '1',
        '--memory', '1G',
        '--overprovisioned',
        '--node-id', '0',
        '--kafka-addr', 'internal://0.0.0.0:9092,external://0.0.0.0:29092',
        '--advertise-kafka-addr', `internal://127.0.0.1:9092,external://127.0.0.1:${this.port}`,
      ])
      .withStartupTimeout(90000)
      .start();

    // Bootstrap Kafka topics
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

    for (const topic of topics) {
      const result = await this.container.exec([
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

    return `127.0.0.1:${this.port}`;
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

  getPort(): number {
    return this.port;
  }
}
