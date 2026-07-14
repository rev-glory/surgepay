import { Injectable } from '@nestjs/common';

import { BaseInboxRepository } from '@surgepay/common';

import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class LedgerInboxRepository extends BaseInboxRepository {
  constructor(prisma: PrismaService) {
    super(prisma.client);
  }
}
