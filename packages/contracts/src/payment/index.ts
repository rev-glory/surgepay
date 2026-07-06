export interface CreatePaymentRequest {
  idempotencyKey: string;
  amount: number;
  currency: string;
  merchantId: string;
  orderId: string;
  paymentMethod: string;
  customerId?: string;
  metadata?: Record<string, unknown>;
}

export interface CreatePaymentResponse {
  paymentId: string;
  status: 'PENDING' | 'COMPLETED' | 'FAILED' | 'REJECTED';
  amount: number;
  currency: string;
  merchantId: string;
  orderId: string;
  correlationId: string;
  createdAt: string;
}
