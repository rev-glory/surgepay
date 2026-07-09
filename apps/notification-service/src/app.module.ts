import { Module } from '@nestjs/common';
import { LoggerModule } from '@surgepay/common';
import { ConfigModule } from '@surgepay/config';
import { MessagingModule } from '@surgepay/common-messaging';

import { PrismaService } from './prisma.service';
import { InboxRepository } from './inbox/inbox.repository';
import { InboxService } from './inbox/inbox.service';

@Module({
  imports: [
    ConfigModule,
    LoggerModule,
    MessagingModule,
  ],
  providers: [
    PrismaService,
    InboxRepository,
    InboxService,
  ],
  exports: [
    PrismaService,
    InboxRepository,
    InboxService,
  ],
})
export class AppModule {}
