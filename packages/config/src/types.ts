export interface DatabaseConfig {
  url: string;
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  database?: string;
  schema?: string;
  ssl?: boolean;
  poolSize: number;
  connectTimeout: number;
  idleTimeout: number;
}

export interface RedisConfig {
  url: string;
  password?: string;
  tls: boolean;
}

export interface KafkaConfig {
  brokers: string[];
  clientId: string;
  ssl: boolean;
  sasl: boolean;
}

export interface HttpConfig {
  port: number;
  host: string;
  apiPrefix: string;
}

export interface LoggingConfig {
  level: 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';
  pretty: boolean;
  serviceName: string;
}

export interface TelemetryConfig {
  endpoint: string;
  serviceName: string;
}

export interface SecurityConfig {
  jwtSecret: string;
  apiKeyHeader: string;
  corsEnabled: boolean;
}
