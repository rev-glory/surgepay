import { InvalidPaymentStateTransitionException } from '../domain/exceptions/invalid-payment-state-transition.exception';
import { PaymentStatus } from '../generated/client';
import { PaymentEntity } from './payment.entity';

describe('PaymentEntity', () => {
  describe('Instantiation', () => {
    it('should initialize a payment starting with PENDING state via static factory', () => {
      const payment = PaymentEntity.create({
        merchantId: 'merchant-1',
        amount: 1000,
        currency: 'USD',
        reference: 'ref-1',
      });

      expect(payment.id).toBeDefined();
      expect(payment.status).toBe(PaymentStatus.PENDING);
      expect(payment.amount).toBe(1000);
      expect(payment.currency).toBe('USD');
      expect(payment.reference).toBe('ref-1');
    });

    it('should not allow direct mutation of the status property', () => {
      const payment = PaymentEntity.create({
        merchantId: 'merchant-1',
        amount: 1000,
        currency: 'USD',
        reference: 'ref-1',
      });

      // Attempting direct mutation should trigger typescript error or runtime error
      // since status only has a getter.
      expect(() => {
        (payment as unknown as Record<string, unknown>).status = PaymentStatus.COMPLETED;
      }).toThrow();
    });
  });

  describe('transitionTo', () => {
    it('should successfully transition from PENDING to PROCESSING', () => {
      const payment = PaymentEntity.create({
        merchantId: 'merchant-1',
        amount: 1000,
        currency: 'USD',
        reference: 'ref-1',
      });

      payment.transitionTo(PaymentStatus.PROCESSING);
      expect(payment.status).toBe(PaymentStatus.PROCESSING);
    });

    it('should transition from PROCESSING to COMPLETED', () => {
      const payment = PaymentEntity.create({
        merchantId: 'merchant-1',
        amount: 1000,
        currency: 'USD',
        reference: 'ref-1',
      });

      payment.transitionTo(PaymentStatus.PROCESSING);
      payment.transitionTo(PaymentStatus.COMPLETED);

      expect(payment.status).toBe(PaymentStatus.COMPLETED);
    });

    it('should throw InvalidPaymentStateTransitionException and not modify state on invalid transition', () => {
      const payment = PaymentEntity.create({
        merchantId: 'merchant-1',
        amount: 1000,
        currency: 'USD',
        reference: 'ref-1',
      });

      expect(() => {
        payment.transitionTo(PaymentStatus.COMPLETED);
      }).toThrow(InvalidPaymentStateTransitionException);

      expect(payment.status).toBe(PaymentStatus.PENDING);
    });
  });
});
