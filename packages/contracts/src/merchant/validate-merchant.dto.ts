export interface ValidateMerchantResponse {
  merchantId: string;
  merchantName: string;
  status: 'ACTIVE' | 'INACTIVE' | 'SUSPENDED';
  permissions: string[];
  webhookEnabled: boolean;
}
