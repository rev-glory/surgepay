import { createHash } from 'crypto';

/**
 * Hashes a plain text API key using SHA-256.
 *
 * @param apiKey The plain text API key.
 * @returns The SHA-256 hex digest of the key.
 */
export function hashApiKey(apiKey: string): string {
  return createHash('sha256').update(apiKey).digest('hex');
}
