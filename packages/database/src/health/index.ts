/**
 * Connectivity checks for PostgreSQL databases using a simple ping query.
 */

interface HealthQueryable {
  $queryRawUnsafe?(query: string, ...values: unknown[]): Promise<unknown>;
  $queryRaw?(query: TemplateStringsArray, ...values: unknown[]): Promise<unknown>;
}

/**
 * Pings the database by executing a simple SELECT 1 raw query.
 * Returns true if connection is successful, false otherwise.
 */
export async function ping(client: HealthQueryable): Promise<boolean> {
  try {
    if (typeof client.$queryRawUnsafe === 'function') {
      await client.$queryRawUnsafe('SELECT 1');
    } else if (typeof client.$queryRaw === 'function') {
      await client.$queryRaw`SELECT 1`;
    } else {
      return false;
    }
    return true;
  } catch (_error) {
    return false;
  }
}

/**
 * Validates if the client has an active connection to the database.
 */
export async function isConnected(client: HealthQueryable): Promise<boolean> {
  return ping(client);
}
