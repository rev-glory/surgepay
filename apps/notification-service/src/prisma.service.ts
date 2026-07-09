import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { getOrCreatePrismaClient, isConnected } from '@surgepay/database';
import { HealthDatabaseClient } from '@surgepay/common';
import { PrismaClient } from '@surgepay/database/generated/notification';

@Injectable()
export class PrismaService extends HealthDatabaseClient implements OnModuleInit, OnModuleDestroy {
  public readonly client: PrismaClient;

  constructor() {
    super();
    this.client = getOrCreatePrismaClient<PrismaClient>(
      'notification',
      PrismaClient as new (options?: unknown) => PrismaClient,
      {
        logQueries: process.env.NODE_ENV === 'development',
      },
    );
  }

  async onModuleInit(): Promise<void> {
    await this.client.$connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.client.$disconnect();
  }

  async isConnected(): Promise<boolean> {
    return isConnected(this.client);
  }
}
