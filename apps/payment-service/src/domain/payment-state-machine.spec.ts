import { InvalidPaymentStateTransitionException } from '@surgepay/common';

import { PaymentStatus } from '../generated/client';
import { PaymentStateMachine } from './payment-state-machine';

describe('PaymentStateMachine', () => {
  const paymentId = 'test-payment-id';

  describe('Valid transitions', () => {
    it('should allow transition from PENDING to PROCESSING', () => {
      const next = PaymentStateMachine.transition(paymentId, PaymentStatus.PENDING, PaymentStatus.PROCESSING);
      expect(next).toBe(PaymentStatus.PROCESSING);
    });

    it('should allow transition from PROCESSING to COMPLETED', () => {
      const next = PaymentStateMachine.transition(paymentId, PaymentStatus.PROCESSING, PaymentStatus.COMPLETED);
      expect(next).toBe(PaymentStatus.COMPLETED);
    });

    it('should allow transition from PROCESSING to FAILED', () => {
      const next = PaymentStateMachine.transition(paymentId, PaymentStatus.PROCESSING, PaymentStatus.FAILED);
      expect(next).toBe(PaymentStatus.FAILED);
    });
  });

  describe('Invalid transitions', () => {
    const testCases = [
      { current: PaymentStatus.PENDING, target: PaymentStatus.PENDING },
      { current: PaymentStatus.PROCESSING, target: PaymentStatus.PROCESSING },
      { current: PaymentStatus.COMPLETED, target: PaymentStatus.COMPLETED },
      { current: PaymentStatus.FAILED, target: PaymentStatus.FAILED },
      { current: PaymentStatus.PENDING, target: PaymentStatus.COMPLETED },
      { current: PaymentStatus.PENDING, target: PaymentStatus.FAILED },
      { current: PaymentStatus.COMPLETED, target: PaymentStatus.PENDING },
      { current: PaymentStatus.COMPLETED, target: PaymentStatus.PROCESSING },
      { current: PaymentStatus.COMPLETED, target: PaymentStatus.FAILED },
      { current: PaymentStatus.FAILED, target: PaymentStatus.PENDING },
      { current: PaymentStatus.FAILED, target: PaymentStatus.PROCESSING },
      { current: PaymentStatus.FAILED, target: PaymentStatus.COMPLETED },
    ];

    testCases.forEach(({ current, target }) => {
      it(`should throw InvalidPaymentStateTransitionException when transitioning from ${current} to ${target}`, () => {
        expect(() => {
          PaymentStateMachine.transition(paymentId, current, target);
        }).toThrow(InvalidPaymentStateTransitionException);

        try {
          PaymentStateMachine.transition(paymentId, current, target);
        } catch (error) {
          expect(error).toBeInstanceOf(InvalidPaymentStateTransitionException);
          const exc = error as InvalidPaymentStateTransitionException;
          expect(exc.paymentId).toBe(paymentId);
          expect(exc.currentStatus).toBe(current);
          expect(exc.attemptedStatus).toBe(target);
          expect(exc.statusCode).toBe(422);
          expect(exc.message).toContain(current);
          expect(exc.message).toContain(target);
        }
      });
    });
  });
});
