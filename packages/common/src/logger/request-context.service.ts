import { Injectable } from '@nestjs/common';

import { RequestContext, RequestContextStore } from './request-context';

@Injectable()
export class RequestContextService {
  getStore(): RequestContextStore | undefined {
    return RequestContext.currentStore();
  }

  get requestId(): string | undefined {
    return RequestContext.requestId;
  }

  get correlationId(): string | undefined {
    return RequestContext.correlationId;
  }

  get sagaId(): string | undefined {
    return RequestContext.sagaId;
  }

  get eventId(): string | undefined {
    return RequestContext.eventId;
  }

  get merchantId(): string | undefined {
    return RequestContext.merchantId;
  }

  get paymentId(): string | undefined {
    return RequestContext.paymentId;
  }
}
