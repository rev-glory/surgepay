import { Injectable } from '@nestjs/common';
import { RecordMetadata } from 'kafkajs';

import {
  KafkaEventProducer,
  LoggerService,
  MalformedEventEnvelopeException,
  TOPIC_REGISTRY,
} from '@surgepay/common';
import {
  BaseEventEnvelope,
  CHECK_PAYOUT_ELIGIBILITY,
  NOTIFY_MERCHANT,
  RECORD_LEDGER_ENTRY,
  RESERVE_BALANCE,
  REVERSE_BALANCE,
  REVERSE_LEDGER_ENTRY,
} from '@surgepay/events';

const SUPPORTED_COMMANDS = new Set<string>([
  RECORD_LEDGER_ENTRY,
  REVERSE_LEDGER_ENTRY,
  CHECK_PAYOUT_ELIGIBILITY,
  RESERVE_BALANCE,
  REVERSE_BALANCE,
  NOTIFY_MERCHANT,
]);

@Injectable()
export class CommandDispatcher {
  constructor(
    private readonly logger: LoggerService,
    private readonly eventProducer: KafkaEventProducer
  ) {
    this.logger.setContext('CommandDispatcher');
  }

  /**
   * Validates a command envelope and dispatches it asynchronously to its Redpanda command topic.
   * Returns Kafka record publishing metadata upon successful broker acknowledgment.
   */
  async dispatch<T>(envelope: BaseEventEnvelope<T>): Promise<RecordMetadata[]> {
    const { eventId, eventType, correlationId, causationId, sagaId, timestamp, version } =
      envelope;

    // 1. Envelope Presence and Empty Checks
    if (
      typeof eventId !== 'string' ||
      !eventId.trim() ||
      typeof eventType !== 'string' ||
      !eventType.trim() ||
      typeof correlationId !== 'string' ||
      !correlationId.trim() ||
      typeof causationId !== 'string' ||
      !causationId.trim() ||
      typeof sagaId !== 'string' ||
      !sagaId.trim() ||
      typeof timestamp !== 'string' ||
      !timestamp.trim()
    ) {
      throw new MalformedEventEnvelopeException(
        'Required command envelope fields are missing or empty'
      );
    }

    // 2. Version Validation
    if (version !== 1) {
      throw new MalformedEventEnvelopeException(
        `Unsupported command version: ${version}. Only version 1 is supported.`
      );
    }

    // 3. Supported Command Type Check
    if (!SUPPORTED_COMMANDS.has(eventType)) {
      throw new MalformedEventEnvelopeException(`Unsupported command type: ${eventType}`);
    }

    // 4. Topic Resolution
    const topic = TOPIC_REGISTRY[eventType];
    if (!topic) {
      throw new MalformedEventEnvelopeException(
        `No Kafka topic mapping found for command type: ${eventType}`
      );
    }

    const logContext = {
      eventId,
      eventType,
      correlationId,
      sagaId,
      causationId,
      topic,
    };

    this.logger.info(`Dispatching saga command: ${eventType}`, logContext);

    try {
      // Publish command using the correlation/sagaId as partition key to maintain order
      const result = await this.eventProducer.publish(topic, sagaId, envelope);

      this.logger.info(`Successfully dispatched command: ${eventType}`, {
        ...logContext,
        partition: result[0]?.partition,
        offset: result[0]?.offset,
      });

      return result;
    } catch (err) {
      this.logger.error(`Failed to dispatch command: ${eventType}`, err as Error, logContext);
      throw err;
    }
  }
}
