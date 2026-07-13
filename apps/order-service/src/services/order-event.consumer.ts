import { Injectable } from '@nestjs/common';

import { BaseKafkaConsumer, LoggerService, TOPIC_REGISTRY } from '@surgepay/common';
import { ConfigService } from '@surgepay/config';
import { PAYMENT_INITIATED } from '@surgepay/events';

import { OrderInboxRepository } from '../repositories/inbox.repository';

@Injectable()
export class OrderEventConsumer extends BaseKafkaConsumer {
  protected readonly topic = TOPIC_REGISTRY[PAYMENT_INITIATED] || 'payments.initiated';
  protected readonly groupId = this.config.kafka.consumerGroupId;

  constructor(
    config: ConfigService,
    logger: LoggerService,
    protected readonly inboxRepository: OrderInboxRepository,
  ) {
    super(config, logger);
  }
}
