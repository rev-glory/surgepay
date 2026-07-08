import { Body, Controller, HttpCode, HttpStatus, Post, UsePipes, ValidationPipe } from '@nestjs/common';

import { PrecheckRequestDto } from './dto/precheck-request.dto';
import { PrecheckResponseDto } from './dto/precheck-response.dto';
import { FraudService } from './fraud.service';

@Controller('internal/fraud')
export class FraudController {
  constructor(private readonly fraudService: FraudService) {}

  @Post('precheck')
  @HttpCode(HttpStatus.OK)
  @UsePipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  )
  async precheck(@Body() body: PrecheckRequestDto): Promise<PrecheckResponseDto> {
    return this.fraudService.runPrecheck(body);
  }
}
