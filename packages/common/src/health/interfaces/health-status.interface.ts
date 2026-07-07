export type HealthStatus = 'UP' | 'DOWN' | 'FAILED';

export interface DependencyStatus {
  status: HealthStatus;
  message?: string;
  [key: string]: unknown;
}

export interface HealthIndicatorContract {
  name: string;
  isHealthy(key: string): Promise<Record<string, DependencyStatus>>;
}

export interface LivenessResponse {
  status: HealthStatus;
}

export interface ReadinessResponse {
  status: HealthStatus;
  checks: Record<string, HealthStatus>;
}

export interface OverallHealthResponse {
  status: HealthStatus;
  service: string;
  timestamp: string;
  checks: Record<string, HealthStatus>;
}

