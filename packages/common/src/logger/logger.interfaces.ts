export interface LogContext {
  requestId?: string;
  paymentId?: string;
  correlationId?: string;
  sagaId?: string;
  eventId?: string;
  merchantId?: string;
  serviceName?: string;
  traceId?: string;
  spanId?: string;
  durationMs?: number;
  method?: string;
  path?: string;
  status?: number;
  [key: string]: unknown;
}

export interface ILogger {
  trace(message: string, context?: LogContext): void;
  debug(message: string, context?: LogContext): void;
  info(message: string, context?: LogContext): void;
  warn(message: string, context?: LogContext): void;
  error(message: string, error?: Error | string | unknown, context?: LogContext): void;
  fatal(message: string, error?: Error | string | unknown, context?: LogContext): void;
}
