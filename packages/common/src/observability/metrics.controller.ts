import { Controller, Get, Res } from '@nestjs/common';
import type { Response } from 'express';

import { MetricsService } from './prometheus-metrics.service';

@Controller('metrics')
export class MetricsController {
  constructor(private readonly metricsService: MetricsService) {}

  @Get()
  async getMetrics(@Res() res: Response): Promise<void> {
    res.set('Content-Type', this.metricsService.registry.contentType);
    res.end(await this.metricsService.registry.metrics());
  }
}
