import { Module } from '@nestjs/common';

import { MetricsController } from './metrics.controller';
import { MetricsService } from './prometheus-metrics.service';

@Module({
  controllers: [MetricsController],
  providers: [
    {
      provide: 'CUSTOM_REGISTRY',
      useValue: null,
    },
    MetricsService,
  ],
  exports: [MetricsService],
})
export class MetricsModule {}
