import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsInt, IsISO4217CurrencyCode, IsObject, IsOptional, IsPositive, IsString, IsUUID, Min } from 'class-validator';

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
  @ApiProperty({
    description: 'Unique idempotency key to prevent double charging',
    example: '550e8400-e29b-41d4-a716-446655440000',
  })
  @IsString()
  idempotencyKey!: string;

  @ApiProperty({
    description: 'The transaction amount to process (must be greater than 0)',
    example: 99.99,
  })
  @IsPositive()
  amount!: number;

  @ApiProperty({
    description: 'Three-letter ISO 4217 currency code',
    example: 'USD',
  })
  @IsISO4217CurrencyCode()
  currency!: string;

  @ApiProperty({
    description: 'Unique UUID identifier of the merchant making the request',
    example: '9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d',
  })
  @IsUUID(4)
  merchantId!: string;

  @ApiProperty({
    description: 'Unique UUID order reference identifier',
    example: 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
  })
  @IsUUID(4)
  orderId!: string;

  @ApiProperty({
    description: 'The payment method type used for the transaction',
    example: 'card',
  })
  @IsString()
  paymentMethod!: string;

  @ApiPropertyOptional({
    description: 'Optional customer identifier',
    example: 'cust_12345',
  })
  @IsOptional()
  @IsString()
  customerId?: string;

  @ApiPropertyOptional({
    description: 'Optional arbitrary metadata associated with the transaction',
    example: { department: 'sales', item_id: 'prod_99' },
  })
  @IsOptional()
  @IsObject()
  metadata?: Record<string, unknown>;
}
