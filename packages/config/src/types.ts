export interface DatabaseConfig {
  url: string;
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  database?: string;
  schema?: string;
  ssl?: boolean;
}

export interface RedisConfig {
  host: string;
  port: number;
  password?: string;
  db?: number;
  keyPrefix?: string;
}

export interface KafkaConfig {
  brokers: string[];
  clientId: string;
  groupId: string;
  ssl?: boolean;
  connectionTimeout?: number;
  requestTimeout?: number;
}

export interface LoggingConfig {
  level: 'debug' | 'info' | 'warn' | 'error';
  format: 'json' | 'pretty';
  colorize?: boolean;
}

export interface OpenTelemetryConfig {
  serviceName: string;
  enabled: boolean;
  collectorUrl: string;
  tracesEndpoint?: string;
  metricsEndpoint?: string;
}

export interface HttpConfig {
  port: number;
  host: string;
  corsOrigin?: string | string[];
}
