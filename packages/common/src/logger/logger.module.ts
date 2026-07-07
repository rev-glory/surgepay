import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { LoggerModule as PinoLoggerModule } from 'nestjs-pino';

import { ConfigModule, ConfigService } from '@surgepay/config';

import { ExceptionLoggingFilter } from './exception-logging.filter';
import { LoggerService } from './logger.service';
import { LoggingInterceptor } from './logging.interceptor';
import { LoggingMiddleware } from './logging.middleware';
import { getPinoConfig } from './pino.config';
import { RequestContextService } from './request-context.service';

@Module({
  imports: [
    PinoLoggerModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => getPinoConfig(configService),
    }),
  ],
  providers: [LoggerService, RequestContextService, LoggingInterceptor, ExceptionLoggingFilter],
  exports: [
    LoggerService,
    RequestContextService,
    LoggingInterceptor,
    ExceptionLoggingFilter,
    PinoLoggerModule,
  ],
})
export class LoggerModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    // Apply LoggingMiddleware globally across all routes to construct Request Context Storage
    consumer.apply(LoggingMiddleware).forRoutes('*');
  }
}
