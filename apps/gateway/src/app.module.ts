import { MiddlewareConsumer, Module, NestModule, ValidationPipe } from '@nestjs/common';
import { APP_FILTER, APP_INTERCEPTOR, APP_PIPE } from '@nestjs/core';

import { ExceptionLoggingFilter, HealthModule, LoggerModule, LoggingInterceptor, LoggingMiddleware } from '@surgepay/common';
import { CommonHttpModule } from '@surgepay/common-http';
import { ConfigModule } from '@surgepay/config';

import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AuthMiddleware } from './auth/auth.middleware';
import { AuthModule } from './auth/auth.module';
import { PaymentController } from './controllers/payment.controller';
import { TestController } from './controllers/test.controller';
import { IdempotencyInterceptor } from './idempotency/idempotency.interceptor';
import { IdempotencyModule } from './idempotency/idempotency.module';
import { RateLimitMiddleware } from './rate-limit/rate-limit.middleware';
import { RateLimitModule } from './rate-limit/rate-limit.module';
import { GatewayPaymentProxyService } from './services/gateway-payment-proxy.service';

@Module({
  imports: [ConfigModule, LoggerModule, CommonHttpModule, HealthModule, AuthModule, RateLimitModule, IdempotencyModule],
  controllers: [AppController, TestController, PaymentController],
  providers: [
    AppService,
    GatewayPaymentProxyService,
    {
      provide: APP_FILTER,
      useClass: ExceptionLoggingFilter,
    },
    {
      provide: APP_INTERCEPTOR,
      useClass: LoggingInterceptor,
    },
    {
      provide: APP_INTERCEPTOR,
      useClass: IdempotencyInterceptor,
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
    // 1. Establish request tracing context first
    consumer.apply(LoggingMiddleware).forRoutes('*');

    // 2. Apply security and rate limiting middlewares
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


