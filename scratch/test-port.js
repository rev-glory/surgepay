const { GenericContainer } = require('testcontainers');

async function run() {
  console.log('Starting container...');
  const container = await new GenericContainer('redis:alpine')
    .withExposedPorts({ container: 6379, host: 26379 })
    .start();

  const portBefore = container.getMappedPort(6379);
  console.log(`Port before restart: ${portBefore}`);

  console.log('Restarting container...');
  await container.restart();

  const portAfter = container.getMappedPort(6379);
  console.log(`Port after restart: ${portAfter}`);

  await container.stop();
}

run().catch(console.error);
