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
  consumerGroupId: string;
  consumerRetryLimit: number;
}

export interface HttpConfig {
  port: number;
  host: string;
  apiPrefix: string;
  keepAlive?: boolean;
  maxSockets?: number;
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

export interface ServicesConfig {
  gatewayUrl: string;
  merchantServiceUrl: string;
  paymentServiceUrl: string;
  orderServiceUrl: string;
  ledgerServiceUrl: string;
  balanceServiceUrl: string;
  notificationServiceUrl: string;
  fraudServiceUrl: string;
  internalRequestTimeout: number;
  internalRequestRetries: number;
  internalRequestRetryDelay: number;
}

export interface OutboxConfig {
  pollingInterval: number;
  batchSize: number;
  retryLimit: number;
  publishTimeout: number;
  retentionDays: number;
  staleTimeoutMs: number;
  maxInFlightMessages: number;
  flushInterval: number;
}

export interface SagaConfig {
  scanIntervalMs: number;
  stepTimeoutMs: number;
  retryBaseDelayMs: number;
  retryMaxDelayMs: number;
  maxRetryAttempts: number;
  batchSize: number;
  handoffTimeoutMs: number;
}

