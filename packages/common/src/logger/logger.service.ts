import { Injectable, LoggerService as NestLoggerService, Scope } from '@nestjs/common';
import { context,trace } from '@opentelemetry/api';
import { PinoLogger } from 'nestjs-pino';

import { ILogger, LogContext } from './logger.interfaces';
import { RequestContext } from './request-context';

@Injectable({ scope: Scope.TRANSIENT })
export class LoggerService implements NestLoggerService, ILogger {
  constructor(private readonly pinoLogger: PinoLogger) {}

  setContext(contextName: string): void {
    this.pinoLogger.setContext(contextName);
  }

  private enrichContext(ctx?: LogContext): LogContext {
    const store = RequestContext.currentStore() || {};
    const activeSpan = trace.getSpan(context.active());
    const otelContext = activeSpan
      ? {
          traceId: activeSpan.spanContext().traceId,
          spanId: activeSpan.spanContext().spanId,
        }
      : {};

    return {
      ...store,
      ...otelContext,
      ...ctx,
    };
  }

  trace(message: string, context?: LogContext): void {
    this.pinoLogger.trace(this.enrichContext(context), message);
  }

  debug(message: string, context?: LogContext): void;
  debug(message: unknown, context?: string): void;
  debug(message: unknown, ...args: unknown[]): void {
    const ctx =
      args.length > 0 && typeof args[0] === 'object' && args[0] !== null
        ? (args[0] as LogContext)
        : typeof args[0] === 'string'
          ? { context: args[0] }
          : {};
    this.pinoLogger.debug(this.enrichContext(ctx), typeof message === 'string' ? message : String(message));
  }

  info(message: string, context?: LogContext): void {
    this.pinoLogger.info(this.enrichContext(context), message);
  }

  warn(message: string, context?: LogContext): void;
  warn(message: unknown, context?: string): void;
  warn(message: unknown, ...args: unknown[]): void {
    const ctx =
      args.length > 0 && typeof args[0] === 'object' && args[0] !== null
        ? (args[0] as LogContext)
        : typeof args[0] === 'string'
          ? { context: args[0] }
          : {};
    this.pinoLogger.warn(this.enrichContext(ctx), typeof message === 'string' ? message : String(message));
  }

  error(message: string, error?: Error | string | unknown, context?: LogContext): void;
  error(message: unknown, stack?: string, context?: string): void;
  error(message: unknown, ...args: unknown[]): void {
    let err: unknown = undefined;
    let ctx: LogContext = {};
    const msg = typeof message === 'string' ? message : String(message);

    if (args.length === 1) {
      if (args[0] instanceof Error || typeof args[0] === 'string') {
        err = args[0];
      } else if (typeof args[0] === 'object' && args[0] !== null) {
        ctx = args[0] as LogContext;
      }
    } else if (args.length === 2) {
      err = args[0];
      if (typeof args[1] === 'object' && args[1] !== null) {
        ctx = args[1] as LogContext;
      } else if (typeof args[1] === 'string') {
        ctx = { context: args[1] };
      }
    }

    const enriched = this.enrichContext(ctx);
    if (err) {
      if (err instanceof Error) {
        enriched.err = {
          message: err.message,
          stack: err.stack,
          name: err.name,
        };
      } else {
        enriched.error = err;
      }
    }
    this.pinoLogger.error(enriched, msg);
  }

  fatal(message: string, error?: Error | string | unknown, context?: LogContext): void;
  fatal(message: unknown, stack?: string, context?: string): void;
  fatal(message: unknown, ...args: unknown[]): void {
    let err: unknown = undefined;
    let ctx: LogContext = {};
    const msg = typeof message === 'string' ? message : String(message);

    if (args.length === 1) {
      if (args[0] instanceof Error || typeof args[0] === 'string') {
        err = args[0];
      } else if (typeof args[0] === 'object' && args[0] !== null) {
        ctx = args[0] as LogContext;
      }
    } else if (args.length === 2) {
      err = args[0];
      if (typeof args[1] === 'object' && args[1] !== null) {
        ctx = args[1] as LogContext;
      } else if (typeof args[1] === 'string') {
        ctx = { context: args[1] };
      }
    }

    const enriched = this.enrichContext(ctx);
    if (err) {
      if (err instanceof Error) {
        enriched.err = {
          message: err.message,
          stack: err.stack,
          name: err.name,
        };
      } else {
        enriched.error = err;
      }
    }
    this.pinoLogger.fatal(enriched, msg);
  }

  // NestLoggerService implementations
  log(message: unknown, ...optionalParams: unknown[]): void {
    const ctx = this.extractContext(optionalParams);
    this.info(typeof message === 'string' ? message : String(message), ctx);
  }

  verbose(message: unknown, ...optionalParams: unknown[]): void {
    const ctx = this.extractContext(optionalParams);
    this.trace(typeof message === 'string' ? message : String(message), ctx);
  }

  private extractContext(optionalParams: unknown[]): LogContext {
    if (optionalParams.length === 0) return {};
    const lastParam = optionalParams[optionalParams.length - 1];
    if (typeof lastParam === 'object' && lastParam !== null) {
      return lastParam as LogContext;
    }
    if (typeof lastParam === 'string') {
      return { context: lastParam };
    }
    return {};
  }
}
