import { ValidationPipe } from '@nestjs/common';
import type { ValidationError as ClassValidatorError } from 'class-validator';

import type { ValidationErrorDetail } from '@surgepay/contracts';

import { ValidationError } from '../errors';

export class AppValidationPipe extends ValidationPipe {
  constructor() {
    super({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
      exceptionFactory: (errors) => {
        const formattedErrors = errors.flatMap((err) => {
          const parseError = (error: ClassValidatorError): ValidationErrorDetail[] => {
            const details: ValidationErrorDetail[] = [];
            if (error.constraints) {
              for (const [rule, message] of Object.entries(error.constraints)) {
                details.push({
                  field: error.property,
                  rejectedValue: error.value !== undefined ? error.value : null,
                  rule,
                  message: message as string,
                });
              }
            }
            if (error.children && error.children.length > 0) {
              for (const child of error.children) {
                details.push(...parseError(child));
              }
            }
            return details;
          };
          return parseError(err);
        });

        return new ValidationError('Validation failed', formattedErrors);
      },
    });
  }
}
