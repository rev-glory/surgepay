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
export declare function ping(client: HealthQueryable): Promise<boolean>;
/**
 * Validates if the client has an active connection to the database.
 */
export declare function isConnected(client: HealthQueryable): Promise<boolean>;
export {};
