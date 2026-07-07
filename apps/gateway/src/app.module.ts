import { MiddlewareConsumer, Module, NestModule, ValidationPipe } from '@nestjs/common';
import { APP_FILTER, APP_INTERCEPTOR, APP_PIPE } from '@nestjs/core';

import { ExceptionLoggingFilter, HealthModule, LoggerModule, LoggingInterceptor } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';

import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AuthMiddleware } from './auth/auth.middleware';
import { AuthModule } from './auth/auth.module';
import { RateLimitMiddleware } from './rate-limit/rate-limit.middleware';
import { RateLimitModule } from './rate-limit/rate-limit.module';

@Module({
  imports: [ConfigModule, LoggerModule, HealthModule, AuthModule, RateLimitModule],
  controllers: [AppController],
  providers: [
    AppService,
    {
      provide: APP_FILTER,
      useClass: ExceptionLoggingFilter,
    },
    {
      provide: APP_INTERCEPTOR,
      useClass: LoggingInterceptor,
    },
    {
      provide: APP_PIPE,
      useFactory: () =>
        new ValidationPipe({
          whitelist: true,
          forbidNonWhitelisted: true,
          transform: true,
        }),
    },
  ],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer
      .apply(AuthMiddleware, RateLimitMiddleware)
      .exclude(
        'health',
        'health/live',
        'health/ready',
        'api/v1/health',
        'api/v1/health/live',
        'api/v1/health/ready',
      )
      .forRoutes('*');
  }
}

