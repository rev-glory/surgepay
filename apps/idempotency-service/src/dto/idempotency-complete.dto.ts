import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { IsInt, IsNotEmpty, IsObject, IsOptional, IsString } from 'class-validator';

export class IdempotencyCompleteDto {
  @ApiProperty({
    description: 'Unique UUID of the merchant initiating the request',
    example: '9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d',
  })
  @IsString()
  @IsNotEmpty()
  merchantId!: string;

  @ApiProperty({
    description: 'Client-provided idempotency key used to track the request lifecycle',
    example: '550e8400-e29b-41d4-a716-446655440000',
  })
  @IsString()
  @IsNotEmpty()
  idempotencyKey!: string;

  @ApiProperty({
    description: 'The identifier of the node/request context currently owning the lock',
    example: 'req_1234567890',
  })
  @IsString()
  @IsNotEmpty()
  ownerId!: string;

  @ApiProperty({
    description: 'SHA-256 hash of the initial request body used to detect modifications upon retry',
    example: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
  })
  @IsString()
  @IsNotEmpty()
  requestHash!: string;

  @ApiProperty({
    description: 'The HTTP status code returned by the downstream handler to cache',
    example: 202,
  })
  @IsInt()
  statusCode!: number;

  @ApiPropertyOptional({
    description: 'A key-value map of HTTP response headers to cache',
    example: { 'content-type': 'application/json' },
  })
  @IsObject()
  @IsOptional()
  headers?: Record<string, string>;

  @ApiPropertyOptional({
    description: 'The JSON response body payload returned by the downstream handler to cache',
    example: { success: true, paymentId: 'pay_9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d', status: 'PENDING' },
  })
  @IsOptional()
  body?: Record<string, unknown>;
}
