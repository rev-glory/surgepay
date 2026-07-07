import { Body, Controller, HttpCode, HttpException, HttpStatus, Post } from '@nestjs/common';

@Controller('test-idempotency')
export class TestController {
  @Post()
  @HttpCode(HttpStatus.ACCEPTED) // Returns 202 Accepted
  async test(@Body() body: Record<string, unknown>): Promise<Record<string, unknown>> {
    // Allows testing concurrency by introducing a delay before returning
    if (body.delay && typeof body.delay === 'number') {
      await new Promise((resolve) => setTimeout(resolve, body.delay as number));
    }

    // Allows testing failure cleanup path
    if (body.fail === true || body.fail === 'true') {
      throw new HttpException('Simulated downstream failure', HttpStatus.INTERNAL_SERVER_ERROR);
    }

    return {
      success: true,
      message: 'Processing completed successfully',
      data: body,
      timestamp: new Date().toISOString(),
    };
  }
}
