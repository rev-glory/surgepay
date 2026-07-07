export interface ConnectionPoolConfig {
  connectionLimit?: number;
  poolTimeout?: number; // in seconds
  socketTimeout?: number; // in seconds
  schema?: string;
}

export const DEFAULT_POOL_CONFIG: ConnectionPoolConfig = {
  connectionLimit: 10,
  poolTimeout: 10,
  socketTimeout: 30,
};

/**
 * Appends query parameters for connection pooling and schema isolation to the database connection URL.
 */
export function buildDatabaseUrl(
  baseUrl: string,
  config: ConnectionPoolConfig = DEFAULT_POOL_CONFIG,
): string {
  try {
    const url = new URL(baseUrl);

    if (config.connectionLimit !== undefined && !url.searchParams.has('connection_limit')) {
      url.searchParams.set('connection_limit', config.connectionLimit.toString());
    }
    if (config.poolTimeout !== undefined && !url.searchParams.has('pool_timeout')) {
      url.searchParams.set('pool_timeout', config.poolTimeout.toString());
    }
    if (config.socketTimeout !== undefined && !url.searchParams.has('socket_timeout')) {
      url.searchParams.set('socket_timeout', config.socketTimeout.toString());
    }
    if (config.schema !== undefined && !url.searchParams.has('schema')) {
      url.searchParams.set('schema', config.schema);
    }

    return url.toString();
  } catch (_e) {
    // Fallback if URL is a placeholder or not a valid URL structure
    return baseUrl;
  }
}
