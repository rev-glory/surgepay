import { PinoLogger } from 'nestjs-pino';
import { pino } from 'pino';

import type { LoggingConfig } from '@surgepay/config';

import { LoggerService } from './logger.service';

export class LoggerFactory {
  /**
   * Creates a standalone structured LoggerService.
   * Useful for bootstrapping phases (e.g. main.ts) before NestJS DI is fully initialized.
   */
  static createStandaloneLogger(config: LoggingConfig): LoggerService {
    const pretty = config.pretty;

    const pinoInstance = pino({
      level: config.level,
      
      // Ensure requests and credentials redactions
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
        service: config.serviceName,
      },

      timestamp: pretty
        ? () => `,"time":"${new Date().toISOString()}"`
        : () => `,"timestamp":"${new Date().toISOString()}"`,

      formatters: {
        level: (label: string) => {
          return { level: label };
        },
      },
    });

    const pinoLogger = new PinoLogger({
      pinoHttp: {
        logger: pinoInstance,
      },
    });

    return new LoggerService(pinoLogger);
  }
}
