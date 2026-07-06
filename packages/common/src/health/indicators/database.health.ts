import { Injectable, Optional } from '@nestjs/common';
import { HealthCheckError, HealthIndicator, HealthIndicatorResult } from '@nestjs/terminus';

export abstract class HealthDatabaseClient {
  abstract isConnected(): Promise<boolean>;
}

@Injectable()
export class DatabaseHealthIndicator extends HealthIndicator {
  constructor(
    @Optional()
    private readonly dbClient?: HealthDatabaseClient,
  ) {
    super();
  }

  isConfigured(): boolean {
    return !!this.dbClient;
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    if (!this.dbClient) {
      throw new Error('Database client is not configured');
    }

    try {
      const active = await this.dbClient.isConnected();
      if (!active) {
        throw new Error('Database ping check failed');
      }
      return this.getStatus(key, true);
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      const result = this.getStatus(key, false, { message: err.message });
      throw new HealthCheckError('Database health check failed', result);
    }
  }
}
