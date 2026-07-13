import type { TextMapGetter, TextMapSetter } from '@opentelemetry/api';

export const kafkaTextMapGetter: TextMapGetter<Record<string, unknown>> = {
  get(carrier: Record<string, unknown>, key: string): string | string[] | undefined {
    const value = carrier[key];
    if (value === undefined || value === null) {
      return undefined;
    }
    if (Buffer.isBuffer(value)) {
      return value.toString('utf8');
    }
    if (Array.isArray(value)) {
      return value.map((v) => (Buffer.isBuffer(v) ? v.toString('utf8') : String(v)));
    }
    return String(value);
  },
  keys(carrier: Record<string, unknown>): string[] {
    return Object.keys(carrier);
  },
};

export const kafkaTextMapSetter: TextMapSetter<Record<string, string>> = {
  set(carrier: Record<string, string>, key: string, value: string): void {
    carrier[key] = value;
  },
};
