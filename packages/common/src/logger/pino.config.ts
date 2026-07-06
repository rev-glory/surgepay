import { context, trace } from '@opentelemetry/api';
import type { Params } from 'nestjs-pino';

import type { ConfigService } from '@surgepay/config';

import { RequestContext } from './request-context';

export function getPinoConfig(configService: ConfigService): Params {
  const loggingConfig = configService.logging;
  const pretty = loggingConfig.pretty;

  return {
    pinoHttp: {
      level: loggingConfig.level,
      // Disable default request/response logging to avoid duplication with our LoggingInterceptor
      autoLogging: false,
      
      // Prevent default request/response serialization
      serializers: {
        req: () => undefined,
        res: () => undefined,
      },

      // Redact sensitive payload properties and headers
      redact: {
        paths: [
          'password',
          'req.headers.authorization',
          'req.headers.cookie',
          'req.headers.apiKey',
          'req.headers["x-api-key"]',
          'authorization',
          'cookie',
          'apiKey',
          'secret',
          'token',
          'accessToken',
          'refreshToken',
          '*.password',
          '*.secret',
          '*.token',
          '*.accessToken',
          '*.refreshToken',
          '*.authorization',
          '*.apiKey',
        ],
        censor: '[REDACTED]',
      },

      // Development pretty printing vs production JSON
      transport: pretty
        ? {
            target: 'pino-pretty',
            options: {
              colorize: true,
              singleLine: true,
              translateTime: 'yyyy-mm-dd HH:MM:ss.l o',
            },
          }
        : undefined,

      base: {
        service: loggingConfig.serviceName,
      },

      // Mixin to automatically append request context and OpenTelemetry context to all logs
      mixin() {
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
        };
      },

      // Output "timestamp" in production JSON and "time" in development pretty print
      timestamp: pretty
        ? () => `,"time":"${new Date().toISOString()}"`
        : () => `,"timestamp":"${new Date().toISOString()}"`,

      // Format numeric log level to string (e.g. "info", "debug")
      formatters: {
        level: (label: string) => {
          return { level: label };
        },
      },
    },
  };
}
