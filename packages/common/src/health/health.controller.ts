import { Controller, Get, HttpStatus, Res, VERSION_NEUTRAL } from '@nestjs/common';
import { Response } from 'express';

import { HEALTH_STATUS } from './constants';
import { HealthService } from './health.service';

@Controller({
  version: VERSION_NEUTRAL,
})
export class HealthController {
  constructor(private readonly healthService: HealthService) {}

  @Get('health/live')
  async getLive(@Res() res: Response): Promise<Response> {
    const response = await this.healthService.checkLiveness();
    return res.status(HttpStatus.OK).json(response);
  }

  @Get('health/ready')
  async getReady(@Res() res: Response): Promise<Response> {
    const response = await this.healthService.checkReadiness();
    const statusCode =
      response.status === HEALTH_STATUS.UP ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
    return res.status(statusCode).json(response);
  }

  @Get('health')
  async getOverall(@Res() res: Response): Promise<Response> {
    const response = await this.healthService.checkOverallHealth();
    const statusCode =
      response.status === HEALTH_STATUS.UP ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
    return res.status(statusCode).json(response);
  }
}
