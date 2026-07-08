import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';

import { LoggerModule } from '@surgepay/common';

import { FraudController } from './fraud.controller';
import { FraudService } from './fraud.service';
import { AmountThresholdRule } from './rules/amount-threshold.rule';
import { BlacklistedMerchantRule } from './rules/blacklisted-merchant.rule';
import { KnownFraudPatternRule } from './rules/known-fraud-pattern.rule';
import { UnsupportedCurrencyRule } from './rules/unsupported-currency.rule';
import { VelocityThresholdRule } from './rules/velocity-threshold.rule';

@Module({
  imports: [ConfigModule, LoggerModule],
  controllers: [FraudController],
  providers: [
    FraudService,
    AmountThresholdRule,
    UnsupportedCurrencyRule,
    BlacklistedMerchantRule,
    VelocityThresholdRule,
    KnownFraudPatternRule,
    {
      provide: 'FRAUD_RULES',
      useFactory: (
        amountRule: AmountThresholdRule,
        currencyRule: UnsupportedCurrencyRule,
        merchantRule: BlacklistedMerchantRule,
        velocityRule: VelocityThresholdRule,
        knownFraudRule: KnownFraudPatternRule,
      ) => [amountRule, currencyRule, merchantRule, velocityRule, knownFraudRule],
      inject: [
        AmountThresholdRule,
        UnsupportedCurrencyRule,
        BlacklistedMerchantRule,
        VelocityThresholdRule,
        KnownFraudPatternRule,
      ],
    },
  ],
})
export class FraudModule {}
