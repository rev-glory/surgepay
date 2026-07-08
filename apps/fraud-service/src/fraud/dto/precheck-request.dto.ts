import { IsInt, IsNotEmpty, IsPositive, IsString, IsUUID } from 'class-validator';

import { FraudPrecheckRequest } from '@surgepay/contracts';

export class PrecheckRequestDto implements FraudPrecheckRequest {
  @IsNotEmpty()
  @IsUUID(4)
  merchantId!: string;

  @IsNotEmpty()
  @IsInt()
  @IsPositive()
  amount!: number;

  @IsNotEmpty()
  @IsString()
  currency!: string;
}
