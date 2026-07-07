import { Injectable } from '@nestjs/common';

import { ConfigService } from '@surgepay/config';

export enum InternalService {
  GATEWAY = 'gateway',
  MERCHANT = 'merchant-service',
  PAYMENT = 'payment-service',
  ORDER = 'order-service',
  LEDGER = 'ledger-service',
  BALANCE = 'balance-service',
  NOTIFICATION = 'notification-service',
}

@Injectable()
export class ServiceRegistry {
  private readonly registry: Record<InternalService, string>;

  constructor(private readonly configService: ConfigService) {
    const services = this.configService.services;
    this.registry = {
      [InternalService.GATEWAY]: services.gatewayUrl,
      [InternalService.MERCHANT]: services.merchantServiceUrl,
      [InternalService.PAYMENT]: services.paymentServiceUrl,
      [InternalService.ORDER]: services.orderServiceUrl,
      [InternalService.LEDGER]: services.ledgerServiceUrl,
      [InternalService.BALANCE]: services.balanceServiceUrl,
      [InternalService.NOTIFICATION]: services.notificationServiceUrl,
    };
  }

  /**
   * Resolves the configured base URL for a given internal service.
   *
   * @param service The InternalService enum member.
   */
  resolve(service: InternalService): string {
    const url = this.registry[service];
    if (!url) {
      throw new Error(`Service ${service} is not registered in the service registry.`);
    }
    return url;
  }
}
