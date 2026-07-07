import { z } from 'zod';

export const telemetrySchema = z.object({
  OTEL_EXPORTER_OTLP_ENDPOINT: z
    .string()
    .url('OTEL_EXPORTER_OTLP_ENDPOINT must be a valid endpoint URL')
    .default('http://localhost:4318'),
  OTEL_SERVICE_NAME: z.string().optional(),
});
