export function isNonNullObject(val: unknown): val is Record<string, unknown> {
  return typeof val === 'object' && val !== null;
}
