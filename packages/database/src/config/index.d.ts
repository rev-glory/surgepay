export interface ConnectionPoolConfig {
    connectionLimit?: number;
    poolTimeout?: number;
    socketTimeout?: number;
    schema?: string;
}
export declare const DEFAULT_POOL_CONFIG: ConnectionPoolConfig;
/**
 * Appends query parameters for connection pooling and schema isolation to the database connection URL.
 */
export declare function buildDatabaseUrl(baseUrl: string, config?: ConnectionPoolConfig): string;
