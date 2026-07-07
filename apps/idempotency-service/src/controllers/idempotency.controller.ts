import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';

import { IdempotencyCheckDto } from '../dto/idempotency-check.dto';
import { IdempotencyCleanupDto } from '../dto/idempotency-cleanup.dto';
import { IdempotencyCompleteDto } from '../dto/idempotency-complete.dto';
import { CheckResult, IdempotencyService } from '../services/idempotency.service';

@Controller('internal/idempotency')
export class IdempotencyController {
  constructor(private readonly idempotencyService: IdempotencyService) {}

  @Post('check')
  @HttpCode(HttpStatus.OK)
  async check(@Body() dto: IdempotencyCheckDto): Promise<CheckResult> {
    return this.idempotencyService.check(dto.merchantId, dto.idempotencyKey, dto.requestBody);
  }

  @Post('complete')
  @HttpCode(HttpStatus.OK)
  async complete(@Body() dto: IdempotencyCompleteDto): Promise<{ success: boolean }> {
    await this.idempotencyService.complete(
      dto.merchantId,
      dto.idempotencyKey,
      dto.ownerId,
      dto.requestHash,
      dto.statusCode,
      dto.headers || {},
      dto.body || {},
    );
    return { success: true };
  }

  @Post('cleanup')
  @HttpCode(HttpStatus.OK)
  async cleanup(@Body() dto: IdempotencyCleanupDto): Promise<{ success: boolean }> {
    await this.idempotencyService.cleanup(dto.merchantId, dto.idempotencyKey, dto.ownerId);
    return { success: true };
  }
}
