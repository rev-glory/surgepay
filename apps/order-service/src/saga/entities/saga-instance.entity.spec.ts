import { SagaStatus } from '../../generated/client';
import { SagaInstanceEntity } from './saga-instance.entity';

describe('SagaInstanceEntity', () => {
  const correlationId = 'corr_test_12345';
  const paymentId = 'pay_test_998877';

  describe('creation', () => {
    it('should successfully instantiate in LEDGER_PENDING status', () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });

      expect(entity.id).toBe(correlationId);
      expect(entity.correlationId).toBe(correlationId);
      expect(entity.paymentId).toBe(paymentId);
      expect(entity.status).toBe(SagaStatus.LEDGER_PENDING);
      expect(entity.version).toBe(0);
      expect(entity.completedAt).toBeNull();
      expect(entity.startedAt).toBeInstanceOf(Date);
    });

    it('should enforce the invariant that id must equal correlationId', () => {
      expect(() => {
        new SagaInstanceEntity(
          'mismatched_id',
          paymentId,
          correlationId,
          SagaStatus.LEDGER_PENDING,
          0,
          new Date(),
          null,
          new Date(),
          new Date()
        );
      }).toThrow(/Invariant violation: Saga ID.*and Correlation ID.*must have identical values/);
    });
  });

  describe('state transitions', () => {
    it('should allow valid forward flow step-by-step', () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });

      // LEDGER_PENDING -> LEDGER_RECORDED
      entity.transitionTo(SagaStatus.LEDGER_RECORDED);
      expect(entity.status).toBe(SagaStatus.LEDGER_RECORDED);

      // LEDGER_RECORDED -> ELIGIBILITY_PENDING
      entity.transitionTo(SagaStatus.ELIGIBILITY_PENDING);
      expect(entity.status).toBe(SagaStatus.ELIGIBILITY_PENDING);

      // ELIGIBILITY_PENDING -> BALANCE_PENDING
      entity.transitionTo(SagaStatus.BALANCE_PENDING);
      expect(entity.status).toBe(SagaStatus.BALANCE_PENDING);

      // BALANCE_PENDING -> BALANCE_RESERVED
      entity.transitionTo(SagaStatus.BALANCE_RESERVED);
      expect(entity.status).toBe(SagaStatus.BALANCE_RESERVED);

      // BALANCE_RESERVED -> NOTIFICATION_PENDING
      entity.transitionTo(SagaStatus.NOTIFICATION_PENDING);
      expect(entity.status).toBe(SagaStatus.NOTIFICATION_PENDING);

      // NOTIFICATION_PENDING -> NOTIFIED
      entity.transitionTo(SagaStatus.NOTIFIED);
      expect(entity.status).toBe(SagaStatus.NOTIFIED);

      // NOTIFIED -> CLOSED (terminal)
      entity.transitionTo(SagaStatus.CLOSED);
      expect(entity.status).toBe(SagaStatus.CLOSED);
      expect(entity.completedAt).toBeInstanceOf(Date);
    });

    it('should allow transitions into REVERSED state for intermediate failures after compensation', () => {
      // Test ELIGIBILITY_PENDING -> REVERSED -> CLOSED
      const saga1 = SagaInstanceEntity.create({ paymentId, correlationId });
      saga1.transitionTo(SagaStatus.LEDGER_RECORDED);
      saga1.transitionTo(SagaStatus.ELIGIBILITY_PENDING);
      saga1.transitionTo(SagaStatus.REVERSED);
      expect(saga1.status).toBe(SagaStatus.REVERSED);
      saga1.transitionTo(SagaStatus.CLOSED);
      expect(saga1.status).toBe(SagaStatus.CLOSED);

      // Test BALANCE_PENDING -> REVERSED -> CLOSED
      const saga2 = SagaInstanceEntity.create({ paymentId, correlationId });
      saga2.transitionTo(SagaStatus.LEDGER_RECORDED);
      saga2.transitionTo(SagaStatus.ELIGIBILITY_PENDING);
      saga2.transitionTo(SagaStatus.BALANCE_PENDING);
      saga2.transitionTo(SagaStatus.REVERSED);
      expect(saga2.status).toBe(SagaStatus.REVERSED);
      saga2.transitionTo(SagaStatus.CLOSED);
      expect(saga2.status).toBe(SagaStatus.CLOSED);

      // Test BALANCE_RESERVED -> REVERSED -> CLOSED
      const saga3 = SagaInstanceEntity.create({ paymentId, correlationId });
      saga3.transitionTo(SagaStatus.LEDGER_RECORDED);
      saga3.transitionTo(SagaStatus.ELIGIBILITY_PENDING);
      saga3.transitionTo(SagaStatus.BALANCE_PENDING);
      saga3.transitionTo(SagaStatus.BALANCE_RESERVED);
      saga3.transitionTo(SagaStatus.REVERSED);
      expect(saga3.status).toBe(SagaStatus.REVERSED);
      saga3.transitionTo(SagaStatus.CLOSED);
      expect(saga3.status).toBe(SagaStatus.CLOSED);
    });

    it('should reject invalid transitions (skips)', () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });

      // Skips LEDGER_RECORDED
      expect(() => {
        entity.transitionTo(SagaStatus.ELIGIBILITY_PENDING);
      }).toThrow(/Invalid saga state transition/);
    });

    it('should reject backward transitions', () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });
      entity.transitionTo(SagaStatus.LEDGER_RECORDED);
      entity.transitionTo(SagaStatus.ELIGIBILITY_PENDING);

      // Backward to LEDGER_RECORDED
      expect(() => {
        entity.transitionTo(SagaStatus.LEDGER_RECORDED);
      }).toThrow(/Invalid saga state transition/);
    });

    it('should reject transitions from terminal CLOSED state', () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });
      entity.transitionTo(SagaStatus.LEDGER_RECORDED);
      entity.transitionTo(SagaStatus.ELIGIBILITY_PENDING);
      entity.transitionTo(SagaStatus.BALANCE_PENDING);
      entity.transitionTo(SagaStatus.BALANCE_RESERVED);
      entity.transitionTo(SagaStatus.NOTIFICATION_PENDING);
      entity.transitionTo(SagaStatus.NOTIFIED);
      entity.transitionTo(SagaStatus.CLOSED);

      // Attempt to transition out of CLOSED
      expect(() => {
        entity.transitionTo(SagaStatus.LEDGER_PENDING);
      }).toThrow(/Cannot transition from terminal state CLOSED/);
    });

    it('should do nothing if transitioning to the current state', () => {
      const entity = SagaInstanceEntity.create({ paymentId, correlationId });
      entity.transitionTo(SagaStatus.LEDGER_PENDING);
      expect(entity.status).toBe(SagaStatus.LEDGER_PENDING);
    });
  });
});
