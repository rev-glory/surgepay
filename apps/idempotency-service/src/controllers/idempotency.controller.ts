import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';

import { IdempotencyCheckDto } from '../dto/idempotency-check.dto';
import { IdempotencyCleanupDto } from '../dto/idempotency-cleanup.dto';
import { IdempotencyCompleteDto } from '../dto/idempotency-complete.dto';
import { CheckResult, IdempotencyService } from '../services/idempotency.service';

@ApiTags('Internal Idempotency')
@Controller('internal/idempotency')
export class IdempotencyController {
  constructor(private readonly idempotencyService: IdempotencyService) {}

  @Post('check')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Check or acquire idempotency status',
    description: 'Internal endpoint called by the Gateway. Determines if the request has been processed before, is in progress, or is a new request. If new, acquires a lock.',
  })
  @ApiResponse({
    status: 200,
    description: 'Idempotency check completed successfully. Returns lock status or cached response details.',
    schema: {
      type: 'object',
      properties: {
        status: { type: 'string', example: 'MISS', enum: ['MISS', 'IN_PROGRESS', 'COMPLETED'] },
        ownerId: { type: 'string', example: '550e8400-e29b-41d4-a716-446655440000', description: 'Unique ID generated for lock ownership' },
        requestHash: { type: 'string', example: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' },
        statusCode: { type: 'number', example: 202, nullable: true },
        headers: { type: 'object', example: { 'content-type': 'application/json' }, nullable: true },
        body: { type: 'object', example: { success: true, paymentId: 'pay_123' }, nullable: true },
      },
    },
  })
  @ApiResponse({
    status: 400,
    description: 'Bad Request (validation error on input payload).',
  })
  @ApiResponse({
    status: 409,
    description: 'Conflict (an identical request with this key is already in progress).',
  })
  @ApiResponse({
    status: 422,
    description: 'Unprocessable Entity (idempotency key reused but request body hash did not match).',
  })
  async check(@Body() dto: IdempotencyCheckDto): Promise<CheckResult> {
    return this.idempotencyService.check(dto.merchantId, dto.idempotencyKey, dto.requestBody);
  }

  @Post('complete')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Complete request and cache response',
    description: 'Internal endpoint called by the Gateway to save the response payload and status code, releasing the active lock and transitioning status to COMPLETED.',
  })
  @ApiResponse({
    status: 200,
    description: 'The response cache has been successfully stored.',
    schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean', example: true },
      },
    },
  })
  @ApiResponse({
    status: 400,
    description: 'Bad Request (validation error on complete body payload).',
  })
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
