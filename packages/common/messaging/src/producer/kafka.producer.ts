import { Inject, Injectable } from '@nestjs/common';
import { LoggerService } from '@surgepay/common';
import { BaseEventEnvelope } from '@surgepay/events';
import { CompressionTypes, Producer, RecordMetadata } from 'kafkajs';

import { EVENT_SERIALIZER, KAFKA_PRODUCER } from '../kafka.tokens';
import { Serializer } from '../serializer/serializer.interface';
import { IProducer } from './producer.interface';

@Injectable()
export class KafkaProducer implements IProducer {
  private connected = false;

  constructor(
    @Inject(KAFKA_PRODUCER) private readonly rawProducer: Producer,
    @Inject(EVENT_SERIALIZER) private readonly serializer: Serializer,
    @Inject(LoggerService) private readonly logger: LoggerService,
  ) {
    this.logger.setContext('KafkaProducer');
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    this.logger.info('Connecting to Kafka broker...');
    await this.rawProducer.connect();
    this.connected = true;
    this.logger.info('Kafka producer connected successfully.');
  }

  async disconnect(): Promise<void> {
    if (!this.connected) return;
    this.logger.info('Disconnecting from Kafka broker...');
    await this.rawProducer.disconnect();
    this.connected = false;
    this.logger.info('Kafka producer disconnected successfully.');
  }

  isReady(): boolean {
    return this.connected;
  }

  async publish<T = any>(topic: string, event: BaseEventEnvelope<T>): Promise<RecordMetadata[]> {
    if (!this.connected) {
      throw new Error('Kafka producer is not connected');
    }

    const value = this.serializer.serialize(event);
    const startTime = Date.now();

    try {
      this.logger.debug('Publishing message to Kafka', {
        topic,
        eventId: event.eventId,
        eventType: event.eventType,
        correlationId: event.correlationId,
      });

      const metadata = await this.rawProducer.send({
        topic,
        acks: -1, // all acknowledgements
        compression: CompressionTypes.GZIP,
        messages: [
          {
            key: event.eventId,
            value,
            headers: {
              correlationId: event.correlationId,
              causationId: event.causationId,
            },
          },
        ],
      });

      const duration = Date.now() - startTime;
      this.logger.info('Successfully published message to Kafka', {
        topic,
        eventId: event.eventId,
        eventType: event.eventType,
        correlationId: event.correlationId,
        durationMs: duration,
      });

      return metadata;
    } catch (err) {
      const duration = Date.now() - startTime;
      this.logger.error('Failed to publish message to Kafka', err, {
        topic,
        eventId: event.eventId,
        eventType: event.eventType,
        correlationId: event.correlationId,
        durationMs: duration,
      });
      throw err;
    }
  }
}
