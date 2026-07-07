import { IsDefined, IsNotEmpty, IsString } from 'class-validator';

export class IdempotencyCheckDto {
  @IsString()
  @IsNotEmpty()
  merchantId!: string;

  @IsString()
  @IsNotEmpty()
  idempotencyKey!: string;

  @IsDefined()
  requestBody!: Record<string, unknown>;
}
