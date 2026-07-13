import { Injectable, OnModuleDestroy } from '@nestjs/common';

import { LoggerService, RequestContextService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { HttpClient } from './http-client';
import { InternalService, ServiceRegistry } from './registry/service-registry';

@Injectable()
export class ServiceClient implements OnModuleDestroy {
  public readonly gateway: HttpClient;
  
  public readonly merchant: HttpClient;
  public readonly merchantService: HttpClient;
  
  public readonly payment: HttpClient;
  public readonly paymentService: HttpClient;
  
  public readonly order: HttpClient;
  public readonly orderService: HttpClient;
  
  public readonly ledger: HttpClient;
  public readonly ledgerService: HttpClient;
  
  public readonly balance: HttpClient;
  public readonly balanceService: HttpClient;
  
  public readonly notification: HttpClient;
  public readonly notificationService: HttpClient;
  
  public readonly fraud: HttpClient;
  public readonly fraudService: HttpClient;

  constructor(
    private readonly configService: ConfigService,
    private readonly serviceRegistry: ServiceRegistry,
    private readonly requestContext: RequestContextService,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('ServiceClient');

    const services = this.configService.services;
    const timeout = services.internalRequestTimeout;
    const retries = services.internalRequestRetries;
    const retryDelay = services.internalRequestRetryDelay;

    // Helper function to build clients
    const buildClient = (service: InternalService): HttpClient => {
      const baseURL = this.serviceRegistry.resolve(service);
      return new HttpClient(
        service,
        baseURL,
        timeout,
        retries,
        retryDelay,
        this.requestContext,
        this.logger,
      );
    };

    // Instantiate and expose clients
    this.gateway = buildClient(InternalService.GATEWAY);
    
    this.merchant = buildClient(InternalService.MERCHANT);
    this.merchantService = this.merchant;

    this.payment = buildClient(InternalService.PAYMENT);
    this.paymentService = this.payment;

    this.order = buildClient(InternalService.ORDER);
    this.orderService = this.order;

    this.ledger = buildClient(InternalService.LEDGER);
    this.ledgerService = this.ledger;

    this.balance = buildClient(InternalService.BALANCE);
    this.balanceService = this.balance;

    this.notification = buildClient(InternalService.NOTIFICATION);
    this.notificationService = this.notification;

    this.fraud = buildClient(InternalService.FRAUD);
    this.fraudService = this.fraud;

    this.logger.info('ServiceClient initialized successfully with all registered service endpoints.');
  }

  onModuleDestroy(): void {
    this.logger.info('Shutting down ServiceClient HttpClients...');
    this.gateway.destroy();
    this.merchant.destroy();
    this.payment.destroy();
    this.order.destroy();
    this.ledger.destroy();
    this.balance.destroy();
    this.notification.destroy();
    this.fraud.destroy();
  }
}
