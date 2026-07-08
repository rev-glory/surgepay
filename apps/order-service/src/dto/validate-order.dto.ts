import { IsInt, IsNotEmpty, IsString, IsUUID, Min } from 'class-validator';

export class ValidateOrderDto {
  @IsUUID()
  @IsNotEmpty()
  merchantId!: string;

  @IsString()
  @IsNotEmpty()
  reference!: string;

  @IsInt()
  @Min(1)
  amount!: number;

  @IsString()
  @IsNotEmpty()
  currency!: string;
}
