import type { RequestStatus } from '../constants/request-status';

export interface IdempotencyRecord {
  status: RequestStatus;
  ownerId: string;
  requestHash: string;
  statusCode: number | null;
  headers: Record<string, string> | null;
  body: Record<string, unknown> | null;
  createdAt: string;
  expiresAt: string;
}
