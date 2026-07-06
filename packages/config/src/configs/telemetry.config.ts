import { registerAs } from '@nestjs/config';

import type { TelemetryConfig } from '../types';

export default registerAs('telemetry', (): TelemetryConfig => ({
  endpoint: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318',
  serviceName: process.env.OTEL_SERVICE_NAME || process.env.LOG_SERVICE_NAME || 'surgepay-service',
}));
