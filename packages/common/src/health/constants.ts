export const HEALTH_STATUS = {
  UP: 'UP' as const,
  DOWN: 'DOWN' as const,
  FAILED: 'FAILED' as const,
};

export const ENDPOINTS = {
  HEALTH: '/health',
  LIVE: '/health/live',
  READY: '/health/ready',
};

export const TIMEOUTS = {
  DATABASE: 3000,
  REDIS: 2000,
  KAFKA: 5000,
};

export const MESSAGES = {
  HEALTHY: 'Service is healthy',
  UNHEALTHY: 'Service is unhealthy',
  LIVENESS_OK: 'Process is alive',
};

export const DATABASE_CLIENT = 'DATABASE_CLIENT';
