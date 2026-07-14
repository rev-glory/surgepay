import { Test, type TestingModule } from '@nestjs/testing';
import type { RecordMetadata } from 'kafkajs';

import {
  KafkaEventProducer,
  LoggerService,
  MalformedEventEnvelopeException,
} from '@surgepay/common';
import {
  type BaseEventEnvelope,
  CHECK_PAYOUT_ELIGIBILITY,
  NOTIFY_MERCHANT,
  RECORD_LEDGER_ENTRY,
  RESERVE_BALANCE,
  REVERSE_BALANCE,
  REVERSE_LEDGER_ENTRY,
} from '@surgepay/events';

import { CommandDispatcher } from './command.dispatcher';

describe('CommandDispatcher', () => {
  let dispatcher: CommandDispatcher;
  let eventProducerMock: {
    publish: jest.Mock;
  };
  let loggerMock: jest.Mocked<Partial<LoggerService>>;

  beforeEach(async () => {
    eventProducerMock = {
      publish: jest.fn(),
    };
    loggerMock = {
      setContext: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CommandDispatcher,
        { provide: KafkaEventProducer, useValue: eventProducerMock },
        { provide: LoggerService, useValue: loggerMock },
      ],
    }).compile();

    dispatcher = module.get<CommandDispatcher>(CommandDispatcher);
  });

  const validEnvelope: BaseEventEnvelope<Record<string, unknown>> = {
    eventId: 'cmd_12345',
    eventType: RECORD_LEDGER_ENTRY,
    correlationId: 'corr_abcdef',
    causationId: 'evt_998877',
    sagaId: 'corr_abcdef',
    timestamp: new Date().toISOString(),
    version: 1,
    payload: {
      paymentId: 'pay_xyz',
      merchantId: 'merch_456',
      amount: 1000,
      currency: 'USD',
      entryType: 'DEBIT',
      description: 'Saga financial ledger entry',
    },
  };

  it('should initialize and set correct logger context', () => {
    expect(loggerMock.setContext).toHaveBeenCalledWith('CommandDispatcher');
  });

  describe('validation and schema verification', () => {
    it('should reject envelopes with missing core identifiers', async () => {
      const invalid = { ...validEnvelope, eventId: '' };
      await expect(dispatcher.dispatch(invalid)).rejects.toThrow(
        MalformedEventEnvelopeException
      );

      const missingCorr = { ...validEnvelope, correlationId: ' ' };
      await expect(dispatcher.dispatch(missingCorr)).rejects.toThrow(
        MalformedEventEnvelopeException
      );
    });

    it('should reject commands with versions other than 1', async () => {
      const invalidVersion = { ...validEnvelope, version: 2 };
      await expect(dispatcher.dispatch(invalidVersion)).rejects.toThrow(
        MalformedEventEnvelopeException
      );
    });

    it('should reject unsupported event types', async () => {
      const unsupportedType = { ...validEnvelope, eventType: 'InvalidCommandName' };
      await expect(dispatcher.dispatch(unsupportedType)).rejects.toThrow(
        MalformedEventEnvelopeException
      );
    });
  });

  describe('routing verification', () => {
    const routingTestCases = [
      { type: RECORD_LEDGER_ENTRY, expectedTopic: 'ledger.commands' },
      { type: REVERSE_LEDGER_ENTRY, expectedTopic: 'ledger.commands' },
      { type: CHECK_PAYOUT_ELIGIBILITY, expectedTopic: 'risk.commands' },
      { type: RESERVE_BALANCE, expectedTopic: 'balance.commands' },
      { type: REVERSE_BALANCE, expectedTopic: 'balance.commands' },
      { type: NOTIFY_MERCHANT, expectedTopic: 'notification.commands' },
    ];

    routingTestCases.forEach(({ type, expectedTopic }) => {
      it(`should route ${type} to ${expectedTopic}`, async () => {
        const envelope = { ...validEnvelope, eventType: type };
        eventProducerMock.publish.mockResolvedValue([]);

        await dispatcher.dispatch(envelope);

        expect(eventProducerMock.publish).toHaveBeenCalledWith(
          expectedTopic,
          envelope.sagaId,
          envelope
        );
      });
    });
  });

  describe('dispatcher execution and acknowledgment', () => {
    it('should publish to resolved topic and return broker record metadata', async () => {
      const mockMeta: RecordMetadata[] = [
        {
          topicName: 'ledger.commands',
          partition: 1,
          offset: '1005',
          errorCode: 0,
        },
      ];
      eventProducerMock.publish.mockResolvedValue(mockMeta);

      const result = await dispatcher.dispatch(validEnvelope);

      expect(eventProducerMock.publish).toHaveBeenCalledWith(
        'ledger.commands',
        validEnvelope.sagaId,
        validEnvelope
      );
      expect(result).toBe(mockMeta);
      expect(loggerMock.info).toHaveBeenCalledWith(
        expect.stringContaining('Successfully dispatched command'),
        expect.objectContaining({
          eventId: 'cmd_12345',
          eventType: RECORD_LEDGER_ENTRY,
          partition: 1,
          offset: '1005',
        })
      );
    });
  });

  describe('failure propagation and logging', () => {
    it('should log connection timeout and re-throw exception to caller', async () => {
      const kafkaError = new Error('Broker connection timeout');
      eventProducerMock.publish.mockRejectedValue(kafkaError);

      await expect(dispatcher.dispatch(validEnvelope)).rejects.toThrow(
        'Broker connection timeout'
      );

      expect(loggerMock.error).toHaveBeenCalledWith(
        expect.stringContaining('Failed to dispatch command'),
        kafkaError,
        expect.objectContaining({
          eventId: 'cmd_12345',
          eventType: RECORD_LEDGER_ENTRY,
          sagaId: 'corr_abcdef',
          topic: 'ledger.commands',
        })
      );
    });
  });
});
