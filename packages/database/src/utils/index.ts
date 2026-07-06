/**
 * Checks if the thrown error is a Prisma-specific database error.
 */
export function isPrismaError(error: unknown): boolean {
  return (
    error !== null &&
    typeof error === 'object' &&
    'code' in error &&
    typeof (error as Record<string, unknown>)['code'] === 'string' &&
    'clientVersion' in error &&
    typeof (error as Record<string, unknown>)['clientVersion'] === 'string'
  );
}
