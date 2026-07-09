import { GenericContainer, StartedTestContainer } from 'testcontainers';
import * as net from 'net';

async function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as net.AddressInfo).port;
      server.close(() => {
        resolve(port);
      });
    });
    server.on('error', (err) => {
      reject(err);
    });
  });
}

export class RedpandaTestContainer {
  private container: StartedTestContainer | null = null;

  async start(): Promise<string> {
    const freePort = await getFreePort();
    this.container = await new GenericContainer('docker.redpanda.com/redpandadata/redpanda:v23.3.10')
      .withExposedPorts({ container: 9092, host: freePort })
      .withCommand([
        'redpanda',
        'start',
        '--mode',
        'dev-container',
        '--kafka-addr',
        'PLAINTEXT://0.0.0.0:9092',
        '--advertise-kafka-addr',
        `PLAINTEXT://127.0.0.1:${freePort}`,
      ])
      .start();

    return `127.0.0.1:${freePort}`;
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
