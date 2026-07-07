import { IsNotEmpty, IsString } from 'class-validator';

export class IdempotencyCleanupDto {
  @IsString()
  @IsNotEmpty()
  merchantId!: string;

  @IsString()
  @IsNotEmpty()
  idempotencyKey!: string;

  @IsString()
  @IsNotEmpty()
  ownerId!: string;
}
