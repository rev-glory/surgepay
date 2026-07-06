export interface MerchantResponse {
  id: string;
  name: string;
  email: string;
  status: 'ACTIVE' | 'INACTIVE' | 'SUSPENDED';
  payoutSettings: {
    enabled: boolean;
    destination: string;
    schedule: 'IMMEDIATE' | 'DAILY' | 'WEEKLY';
  };
  createdAt: string;
  updatedAt: string;
}
