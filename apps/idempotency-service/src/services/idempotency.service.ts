import { HttpException, HttpStatus, Inject, Injectable } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import { v4 as uuidv4 } from 'uuid';

import { LoggerService } from '@surgepay/common';

import idempotencyConfig from '../config/idempotency.config';
import { RequestStatus } from '../constants/request-status';
import { RedisRepository } from '../repositories/redis.repository';
import { RequestHashService } from './request-hash.service';

export interface CheckResult {
  status: RequestStatus | 'MISS';
  ownerId?: string;
  requestHash?: string;
  statusCode?: number | null;
  headers?: Record<string, string> | null;
  body?: Record<string, unknown> | null;
}

@Injectable()
export class IdempotencyService {
  constructor(
    private readonly redisRepository: RedisRepository,
    private readonly requestHashService: RequestHashService,
    @Inject(idempotencyConfig.KEY)
    private readonly config: ConfigType<typeof idempotencyConfig>,
    private readonly logger: LoggerService,
  ) {
    this.logger.setContext('IdempotencyService');
  }

  /**
   * Checks the idempotency state for a request.
   * - If MISS, atomically acquires the lock and returns status 'MISS' with ownerId.
   * - If IN_PROGRESS, throws a 409 Conflict exception.
   * - If COMPLETED, returns the cached response details.
   * - If request body has changed, throws a 422 Unprocessable Entity exception.
   */
  async check(
    merchantId: string,
    idempotencyKey: string,
    requestBody: Record<string, unknown>,
  ): Promise<CheckResult> {
    const startTime = Date.now();
    const requestHash = this.requestHashService.generate(requestBody);
    const ownerId = uuidv4();
    const ttlHours = this.config.ttlHours;

    this.logger.debug('Executing idempotency check', {
      merchantId,
      idempotencyKey,
      requestHash,
    });

    // 1. Try atomic lock acquisition (NX)
    const acquired = await this.redisRepository.createInProgress(
      merchantId,
      idempotencyKey,
      requestHash,
      ownerId,
      ttlHours,
    );

    if (acquired) {
      const duration = Date.now() - startTime;
      this.logger.info('Idempotency lock acquired (MISS)', {
        merchantId,
        idempotencyKey,
        ownerId,
        requestHash,
        requestStatus: 'MISS',
        durationMs: duration,
      });

      return {
        status: 'MISS',
        ownerId,
        requestHash,
      };
    }

    // 2. Lock not acquired, retrieve existing state
    let record = await this.redisRepository.findRequest(merchantId, idempotencyKey);
    
    // In rare concurrent race conditions, if the key expired right after SETNX failure
    if (!record) {
      this.logger.warn('Key expired immediately after SETNX failure, retrying lock acquisition', {
        merchantId,
        idempotencyKey,
      });
      // Second attempt
      const reAcquired = await this.redisRepository.createInProgress(
        merchantId,
        idempotencyKey,
        requestHash,
        ownerId,
        ttlHours,
      );
      if (reAcquired) {
        return {
          status: 'MISS',
          ownerId,
          requestHash,
        };
      }
      // Re-read if it failed again
      record = await this.redisRepository.findRequest(merchantId, idempotencyKey);
      if (!record) {
        throw new HttpException(
          { error: 'INTERNAL_SERVER_ERROR', message: 'Unable to acquire idempotency lock' },
          HttpStatus.INTERNAL_SERVER_ERROR,
        );
      }
    }

    const duration = Date.now() - startTime;

    // 3. Validate request payload hash
    if (record.requestHash !== requestHash) {
      this.logger.warn('Idempotency key reused with different request payload', {
        merchantId,
        idempotencyKey,
        storedHash: record.requestHash,
        incomingHash: requestHash,
        durationMs: duration,
      });
      throw new HttpException(
        { error: 'IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_REQUEST' },
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }

    // 4. Handle IN_PROGRESS state
    if (record.status === RequestStatus.IN_PROGRESS) {
      this.logger.warn('Duplicate request received while execution is in progress', {
        merchantId,
        idempotencyKey,
        ownerId: record.ownerId,
        durationMs: duration,
      });
      throw new HttpException(
        { error: 'REQUEST_ALREADY_IN_PROGRESS' },
        HttpStatus.CONFLICT,
      );
    }

    // 5. Handle COMPLETED state (Replay response)
    this.logger.info('Idempotency hit! Replaying completed response', {
      merchantId,
      idempotencyKey,
      ownerId: record.ownerId,
      requestHash: record.requestHash,
      requestStatus: 'COMPLETED',
      durationMs: duration,
    });

    return {
      status: RequestStatus.COMPLETED,
      statusCode: record.statusCode,
      headers: record.headers,
      body: record.body,
    };
  }

  /**
   * Completes an in-progress request with the final response.
   * Verifies that the lock ownership matches the ownerId.
   */
  async complete(
    merchantId: string,
    idempotencyKey: string,
    ownerId: string,
    requestHash: string,
    statusCode: number,
    headers: Record<string, string>,
    body: Record<string, unknown>,
  ): Promise<void> {
    const startTime = Date.now();
    const ttlHours = this.config.ttlHours;

    this.logger.debug('Attempting to complete idempotency record', {
      merchantId,
      idempotencyKey,
      ownerId,
      statusCode,
    });

    const success = await this.redisRepository.markCompleted(
      merchantId,
      idempotencyKey,
      ownerId,
      requestHash,
      statusCode,
      headers,
      body,
      ttlHours,
    );

    const duration = Date.now() - startTime;

    if (!success) {
      this.logger.error('Failed to mark request completed: lock ownership token mismatch or expired', undefined, {
        merchantId,
        idempotencyKey,
        ownerId,
        durationMs: duration,
      });
      throw new HttpException(
        { error: 'LOCK_OWNERSHIP_MISMATCH', message: 'Failed to complete: request lock expired or owned by another process' },
        HttpStatus.CONFLICT,
      );
    }

    this.logger.info('Idempotency request marked as COMPLETED successfully', {
      merchantId,
      idempotencyKey,
      ownerId,
      statusCode,
      durationMs: duration,
    });
  }

  /**
   * Safely deletes/cleans up an in-progress request if the ownerId token matches.
   */
  async cleanup(merchantId: string, idempotencyKey: string, ownerId: string): Promise<void> {
    const startTime = Date.now();
    this.logger.debug('Attempting to clean up/release idempotency lock', {
      merchantId,
      idempotencyKey,
      ownerId,
    });

    const success = await this.redisRepository.delete(merchantId, idempotencyKey, ownerId);
    const duration = Date.now() - startTime;

    if (!success) {
      this.logger.warn('Failed to release lock: lock already expired or owned by another process', {
        merchantId,
        idempotencyKey,
        ownerId,
        durationMs: duration,
      });
    } else {
      this.logger.info('Idempotency lock released successfully on failure cleanup', {
        merchantId,
        idempotencyKey,
        ownerId,
        durationMs: duration,
      });
    }
  }
}
