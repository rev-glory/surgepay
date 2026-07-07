import { Controller, Get, HttpStatus, Res } from '@nestjs/common';
import { HealthCheck, HealthCheckService } from '@nestjs/terminus';
import { Response } from 'express';

import { ConfigHealthIndicator, RedisHealthIndicator } from '@surgepay/common';

@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly redis: RedisHealthIndicator,
    private readonly config: ConfigHealthIndicator,
  ) {}

  @Get()
  @HealthCheck()
  async check(@Res() res: Response): Promise<Response> {
    try {
      const result = await this.health.check([
        () => this.redis.isHealthy('redis'),
        () => this.config.isHealthy('configuration'),
      ]);
      return res.status(HttpStatus.OK).json(result);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      return res.status(HttpStatus.SERVICE_UNAVAILABLE).json({ status: 'error', error: message });
    }
  }

  @Get('live')
  async getLive(@Res() res: Response): Promise<Response> {
    return res.status(HttpStatus.OK).json({ status: 'up' });
  }

  @Get('ready')
  @HealthCheck()
  async getReady(@Res() res: Response): Promise<Response> {
    return this.check(res);
  }
}
