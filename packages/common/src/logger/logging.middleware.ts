import { Injectable, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';

import {
  CORRELATION_ID_HEADER,
  EVENT_ID_HEADER,
  MERCHANT_ID_HEADER,
  PAYMENT_ID_HEADER,
  REQUEST_ID_HEADER,
  SAGA_ID_HEADER,
} from './logger.constants';
import { RequestContext, RequestContextStore } from './request-context';

@Injectable()
export class LoggingMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction): void {
    const rawRequestId = req.headers[REQUEST_ID_HEADER] || req.headers['x-request-id'];
    const requestId = typeof rawRequestId === 'string' ? rawRequestId : `req_${uuidv4()}`;

    const rawCorrelationId = req.headers[CORRELATION_ID_HEADER] || req.headers['x-correlation-id'];
    const correlationId = typeof rawCorrelationId === 'string' ? rawCorrelationId : undefined;

    const rawSagaId = req.headers[SAGA_ID_HEADER] || req.headers['x-saga-id'];
    const sagaId = typeof rawSagaId === 'string' ? rawSagaId : undefined;

    const rawEventId = req.headers[EVENT_ID_HEADER] || req.headers['x-event-id'];
    const eventId = typeof rawEventId === 'string' ? rawEventId : undefined;

    const rawMerchantId = req.headers[MERCHANT_ID_HEADER] || req.headers['x-merchant-id'];
    const merchantId = typeof rawMerchantId === 'string' ? rawMerchantId : undefined;

    const rawPaymentId = req.headers[PAYMENT_ID_HEADER] || req.headers['x-payment-id'];
    const paymentId = typeof rawPaymentId === 'string' ? rawPaymentId : undefined;

    const store: RequestContextStore = {
      requestId,
      correlationId,
      sagaId,
      eventId,
      merchantId,
      paymentId,
    };

    // Attach Request ID and correlation context to the response headers
    res.setHeader(REQUEST_ID_HEADER, requestId);
    if (correlationId) {
      res.setHeader(CORRELATION_ID_HEADER, correlationId);
    }

    RequestContext.run(store, () => {
      next();
    });
  }
}
