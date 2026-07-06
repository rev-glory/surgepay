export function now(): Date {
  return new Date();
}

export function currentUtcTimestamp(): number {
  return Date.now();
}

export function toIsoString(date?: Date): string {
  return (date ?? new Date()).toISOString();
}
