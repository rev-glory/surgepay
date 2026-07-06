export interface OrderValidationResponse {
  orderId: string;
  isValid: boolean;
  payable: boolean;
  amount: number;
  currency: string;
  reason?: string;
}
