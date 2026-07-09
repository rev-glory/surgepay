import { Injectable } from '@nestjs/common';

@Injectable()
export class BackpressureController {
  private activeMessagesCount = 0;
  private readonly maxInFlight: number;

  constructor(maxInFlight: number) {
    this.maxInFlight = maxInFlight;
  }

  /**
   * Acquires capacity for a batch of messages.
   * If adding this batch exceeds maxInFlight, it blocks and polls until capacity frees up.
   */
  async acquire(messageCount: number): Promise<void> {
    while (this.activeMessagesCount + messageCount > this.maxInFlight && this.activeMessagesCount > 0) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    this.activeMessagesCount += messageCount;
  }

  /**
   * Releases capacity for a batch of messages.
   */
  release(messageCount: number): void {
    this.activeMessagesCount -= messageCount;
    if (this.activeMessagesCount < 0) {
      this.activeMessagesCount = 0;
    }
  }

  /**
   * Returns the current number of in-flight messages.
   */
  getActiveMessagesCount(): number {
    return this.activeMessagesCount;
  }
}
