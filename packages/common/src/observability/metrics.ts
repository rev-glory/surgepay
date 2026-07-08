import { metrics } from '@opentelemetry/api';

const meter = metrics.getMeter('surgepay-synchronous-pipeline');

export const paymentRequestDuration = meter.createHistogram('payment_request_duration_ms', {
  description: 'Total request duration latency',
  unit: 'ms',
});

export const orderValidationDuration = meter.createHistogram('order_validation_duration_ms', {
  description: 'Order validation service call duration',
  unit: 'ms',
});

export const fraudPrecheckDuration = meter.createHistogram('fraud_precheck_duration_ms', {
  description: 'Fraud pre-check service call duration',
  unit: 'ms',
});

export const paymentTransactionDuration = meter.createHistogram('payment_transaction_duration_ms', {
  description: 'Database transaction duration',
  unit: 'ms',
});
