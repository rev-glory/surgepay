export const MERCHANT_FIXTURES = {
  active: {
    apiKey: 'sp_active_key_123',
    name: 'Acme active merchant',
    email: 'active@acme.com',
    status: 'ACTIVE',
    permissions: ['payment:create'],
    webhookEnabled: true,
  },
  disabled: {
    apiKey: 'sp_disabled_key_123',
    name: 'Acme disabled merchant',
    email: 'disabled@acme.com',
    status: 'INACTIVE',
    permissions: ['payment:create'],
    webhookEnabled: true,
  },
  revoked: {
    apiKey: 'sp_revoked_key_123',
    name: 'Acme revoked key merchant',
    email: 'revoked@acme.com',
    status: 'ACTIVE',
    permissions: ['payment:create'],
    webhookEnabled: true,
  },
  invalid: {
    apiKey: 'sp_invalid_key_999',
  },
};
