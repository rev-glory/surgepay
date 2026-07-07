import { Type } from 'class-transformer';
import { IsInt, IsISO4217CurrencyCode, IsObject,IsOptional, IsPositive, IsString, IsUUID, Min } from 'class-validator';

import { CreatePaymentRequest } from '@surgepay/contracts';

export class PaginationQueryParams {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  page?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  limit?: number;
}

export class CursorQueryParams {
  @IsOptional()
  @IsString()
  cursor?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  limit?: number;
}

export class CreatePaymentRequestDto implements CreatePaymentRequest {
  @IsString()
  idempotencyKey!: string;

  @IsPositive()
  amount!: number;

  @IsISO4217CurrencyCode()
  currency!: string;

  @IsUUID(4)
  merchantId!: string;

  @IsUUID(4)
  orderId!: string;

  @IsString()
  paymentMethod!: string;

  @IsOptional()
  @IsString()
  customerId?: string;

  @IsOptional()
  @IsObject()
  metadata?: Record<string, unknown>;
}
