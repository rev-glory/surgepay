import { IsIn, IsNotEmpty, IsNumber, IsPositive, IsString, MaxLength } from 'class-validator';

export const SUPPORTED_CURRENCIES = ['INR', 'USD', 'EUR', 'GBP'] as const;
export type SupportedCurrency = typeof SUPPORTED_CURRENCIES[number];

export class CreatePaymentRequestDto {
  @IsNotEmpty()
  @IsNumber()
  @IsPositive()
  amount!: number;

  @IsNotEmpty()
  @IsString()
  @IsIn(SUPPORTED_CURRENCIES, {
    message: `Currency must be one of: ${SUPPORTED_CURRENCIES.join(', ')}`,
  })
  currency!: string;

  @IsNotEmpty()
  @IsString()
  @MaxLength(255)
  reference!: string;
}
