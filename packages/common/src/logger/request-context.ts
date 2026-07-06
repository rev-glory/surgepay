import { AsyncLocalStorage } from 'async_hooks';

export interface RequestContextStore {
  requestId: string;
  correlationId?: string;
  sagaId?: string;
  eventId?: string;
  merchantId?: string;
  paymentId?: string;
}

export class RequestContext {
  private static readonly storage = new AsyncLocalStorage<RequestContextStore>();

  static run<T>(store: RequestContextStore, callback: () => T): T {
    return this.storage.run(store, callback);
  }

  static currentStore(): RequestContextStore | undefined {
    return this.storage.getStore();
  }

  static get requestId(): string | undefined {
    return this.currentStore()?.requestId;
  }

  static get correlationId(): string | undefined {
    return this.currentStore()?.correlationId;
  }

  static get sagaId(): string | undefined {
    return this.currentStore()?.sagaId;
  }

  static get eventId(): string | undefined {
    return this.currentStore()?.eventId;
  }

  static get merchantId(): string | undefined {
    return this.currentStore()?.merchantId;
  }

  static get paymentId(): string | undefined {
    return this.currentStore()?.paymentId;
  }
}
