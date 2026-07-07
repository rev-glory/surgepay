import { ApiProperty } from '@nestjs/swagger';
import { IsDefined, IsNotEmpty, IsString } from 'class-validator';

export class IdempotencyCheckDto {
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
    description: 'The raw HTTP request body payload to hash for matching validation',
    example: { amount: 99.99, currency: 'USD', orderId: 'f47ac10b-58cc-4372-a567-0e02b2c3d479' },
  })
  @IsDefined()
  requestBody!: Record<string, unknown>;
}
