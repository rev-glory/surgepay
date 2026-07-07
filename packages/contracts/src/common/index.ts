export interface SuccessResponse<T> {
  success: true;
  data: T;
}

export type ApiResponse<T> = SuccessResponse<T>;

export interface PaginationMetadata {
  total: number;
  page: number;
  limit: number;
  totalPages: number;
  hasNext: boolean;
  hasPrevious: boolean;
}

export interface PaginatedResponse<T> {
  success: true;
  data: T[];
  meta: PaginationMetadata;
}

export interface CursorMetadata {
  nextCursor?: string;
  prevCursor?: string;
  pageSize: number;
  hasNext: boolean;
  hasPrevious: boolean;
}

export interface CursorPaginatedResponse<T> {
  success: true;
  data: T[];
  meta: CursorMetadata;
}

export interface HealthResponse {
  status: 'UP' | 'DOWN' | 'FAILED';
  timestamp: string;
  service: string;
  checks?: Record<string, 'UP' | 'DOWN' | 'FAILED'>;
}
