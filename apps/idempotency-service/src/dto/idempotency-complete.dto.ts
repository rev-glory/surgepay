import { IsInt, IsNotEmpty, IsObject, IsOptional, IsString } from 'class-validator';

export class IdempotencyCompleteDto {
  @IsString()
  @IsNotEmpty()
  merchantId!: string;

  @IsString()
  @IsNotEmpty()
  idempotencyKey!: string;

  @IsString()
  @IsNotEmpty()
  ownerId!: string;

  @IsString()
  @IsNotEmpty()
  requestHash!: string;

  @IsInt()
  statusCode!: number;

  @IsObject()
  @IsOptional()
  headers?: Record<string, string>;

  @IsOptional()
  body?: Record<string, unknown>;
}
