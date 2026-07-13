import { Test, type TestingModule } from '@nestjs/testing';

import { LoggerService } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';

import { OutboxRelayService } from './relay.service';
import { OutboxScheduler } from './scheduler';

describe('OutboxScheduler', () => {
  let scheduler: OutboxScheduler;
  let relayService: jest.Mocked<OutboxRelayService>;
  let config: jest.Mocked<ConfigService>;
  let logger: jest.Mocked<LoggerService>;

  beforeEach(async () => {
    jest.useFakeTimers();

    relayService = {
      processBatch: jest.fn(),
    } as unknown as jest.Mocked<OutboxRelayService>;

    config = {
      outbox: {
        pollingInterval: 500,
        batchSize: 100,
        retryLimit: 3,
        publishTimeout: 5000,
      },
    } as unknown as jest.Mocked<ConfigService>;

    logger = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    } as unknown as jest.Mocked<LoggerService>;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OutboxScheduler,
        { provide: OutboxRelayService, useValue: relayService },
        { provide: ConfigService, useValue: config },
        { provide: LoggerService, useValue: logger },
      ],
    }).compile();

    scheduler = module.get<OutboxScheduler>(OutboxScheduler);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('should start polling on bootstrap and trigger processBatch sequentially', async () => {
    relayService.processBatch.mockResolvedValue(undefined);

    await scheduler.onApplicationBootstrap();

    expect(logger.info).toHaveBeenCalledWith('Outbox Relay Scheduler starting...', expect.any(Object));

    // Fast-forward time to trigger the first poll
    jest.advanceTimersByTime(500);
    await Promise.resolve();
    expect(relayService.processBatch).toHaveBeenCalledTimes(1);

    // Fast-forward again to trigger the second poll
    jest.advanceTimersByTime(500);
    await Promise.resolve();
    expect(relayService.processBatch).toHaveBeenCalledTimes(2);
  });

  it('should prevent overlapping executions by skipping runs when processBatch is still active', async () => {
    // Mock processBatch to run slowly (not resolving immediately)
    let resolveProcess: () => void = () => {};
    const slowProcessPromise = new Promise<void>((resolve) => {
      resolveProcess = resolve;
    });
    relayService.processBatch.mockReturnValue(slowProcessPromise);

    await scheduler.onApplicationBootstrap();

    // First timer fire -> starts polling
    jest.advanceTimersByTime(500);
    expect(relayService.processBatch).toHaveBeenCalledTimes(1);

    // Trigger a second poll manually while the first is still active to simulate overlapping schedule execution
    scheduler['scheduleNextPoll']();
    jest.advanceTimersByTime(500);
    expect(relayService.processBatch).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledWith('Previous polling cycle is still active. Skipping this cycle to prevent overlap.');

    // Now resolve the first process
    resolveProcess();
    await slowProcessPromise;
    await Promise.resolve();

    // After resolving, next cycle can trigger
    jest.advanceTimersByTime(500);
    await Promise.resolve();
    expect(relayService.processBatch).toHaveBeenCalledTimes(2);
  });

  it('should continue running after recoverable failures in outbox polling cycle', async () => {
    // First run fails, second run succeeds
    relayService.processBatch
      .mockRejectedValueOnce(new Error('Database connection failed'))
      .mockResolvedValueOnce(undefined);

    await scheduler.onApplicationBootstrap();

    // First run (fails)
    jest.advanceTimersByTime(500);
    // Wait for promise resolution in the macro-task queue
    await Promise.resolve();
    expect(relayService.processBatch).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith('Recoverable failure in outbox polling cycle. Continuing scheduler.', expect.any(Error));

    // Second run (succeeds)
    jest.advanceTimersByTime(500);
    await Promise.resolve();
    expect(relayService.processBatch).toHaveBeenCalledTimes(2);
  });

  it('should stop cleanly on application shutdown', async () => {
    await scheduler.onApplicationBootstrap();

    // Verify it runs before shutdown
    jest.advanceTimersByTime(500);
    expect(relayService.processBatch).toHaveBeenCalledTimes(1);

    await scheduler.onApplicationShutdown();
    expect(logger.info).toHaveBeenCalledWith('Outbox Relay Scheduler shutting down gracefully...');

    // Timers should not trigger processBatch after shutdown
    jest.advanceTimersByTime(500);
    expect(relayService.processBatch).toHaveBeenCalledTimes(1);
  });
});
