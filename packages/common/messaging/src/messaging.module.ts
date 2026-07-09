import { Module } from '@nestjs/common';
import { LoggerModule, MetricsModule } from '@surgepay/common';
import { ConfigModule, ConfigService } from '@surgepay/config';

import { EVENT_SERIALIZER, KAFKA_PRODUCER, KAFKA_PRODUCER_OPTIONS } from './kafka.tokens';
import { KafkaProducer } from './producer/kafka.producer';
import { createKafkaProducer } from './producer/producer.factory';
import { KafkaProducerOptions } from './producer/producer.options';
import { ProducerService } from './producer/producer.service';
import { EventSerializer } from './serializer/event.serializer';

import { KafkaDlqPublisher } from './consumer/dlq.publisher';

@Module({
  imports: [ConfigModule, LoggerModule, MetricsModule],
  providers: [
    {
      provide: EVENT_SERIALIZER,
      useClass: EventSerializer,
    },
    {
      provide: KAFKA_PRODUCER_OPTIONS,
      useFactory: (configService: ConfigService): KafkaProducerOptions => {
        const kafkaConfig = configService.kafka;
        return {
          kafkaConfig: {
            clientId: kafkaConfig.clientId,
            brokers: kafkaConfig.brokers,
            ssl: kafkaConfig.ssl,
            sasl: kafkaConfig.sasl
              ? {
                  mechanism: 'plain',
                  username: process.env.KAFKA_SASL_USERNAME || '',
                  password: process.env.KAFKA_SASL_PASSWORD || '',
                }
              : undefined,
          },
          requestTimeout: 30000,
          connectionTimeout: 10000,
          retries: 5,
        };
      },
      inject: [ConfigService],
    },
    {
      provide: KAFKA_PRODUCER,
      useFactory: (options: KafkaProducerOptions) => {
        return createKafkaProducer(options);
      },
      inject: [KAFKA_PRODUCER_OPTIONS],
    },
    KafkaProducer,
    ProducerService,
    KafkaDlqPublisher,
  ],
  exports: [ProducerService, EVENT_SERIALIZER, KafkaDlqPublisher],
})
export class MessagingModule {}
