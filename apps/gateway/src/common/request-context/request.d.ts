import type { MerchantContext } from '../../auth/interfaces/merchant-context.interface';

declare global {
  namespace Express {
    interface Request {
      merchant?: MerchantContext;
    }
  }
}
