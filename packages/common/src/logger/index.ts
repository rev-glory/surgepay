export interface LogContext {
  requestId?: string;
  paymentId?: string;
  correlationId?: string;
  sagaId?: string;
  eventId?: string;
  merchantId?: string;
  serviceName?: string;
  [key: string]: unknown;
}

export interface ILogger {
  debug(message: string, context?: LogContext): void;
  info(message: string, context?: LogContext): void;
  warn(message: string, context?: LogContext): void;
  error(message: string, error?: Error | string, context?: LogContext): void;
}
