import { Test, type TestingModule } from '@nestjs/testing';
import { randomUUID } from 'crypto';
import {
  BALANCE_RESERVATION_FAILED,
  BALANCE_RESERVED,
  BALANCE_REVERSED,
  CHECK_PAYOUT_ELIGIBILITY,
  ELIGIBILITY_APPROVED,
  ELIGIBILITY_DENIED,
  LEDGER_ENTRY_RECORDED,
  LEDGER_RECORDING_FAILED,
  LEDGER_REVERSED,
  ORDER_ELIGIBILITY_CONFIRMED,
  PAYMENT_COMPLETED,
  RECORD_LEDGER_ENTRY,
  RESERVE_BALANCE,
  REVERSE_BALANCE,
  REVERSE_LEDGER_ENTRY,
  SAGA_RETRY_REGISTERED,
  SAGA_STEP_EXECUTION_FAILED,
} from '@surgepay/events';

import { AppModule } from '../../../apps/order-service/src/app.module';
import { OrderValidationStatus, SagaStatus, SagaTransitionType } from '../../../apps/order-service/src/generated/client';
import { PrismaService } from '../../../apps/order-service/src/prisma/prisma.service';
import { SagaService } from '../../../apps/order-service/src/saga/saga.service';
import { SagaRepository } from '../../../apps/order-service/src/saga/repositories/saga.repository';
import { SagaTimeoutScanner } from '../../../apps/order-service/src/saga/recovery/saga-timeout.scanner';
import { SagaRecoveryService } from '../../../apps/order-service/src/saga/recovery/saga-recovery.service';
import { OrderInboxRepository } from '../../../apps/order-service/src/repositories/inbox.repository';
import { SagaInstanceEntity } from '../../../apps/order-service/src/saga/entities/saga-instance.entity';
import { SagaPaymentCompletedConsumer } from '../../../apps/order-service/src/saga/handlers/payment-completed.handler';
import { SagaOrderEventsConsumer } from '../../../apps/order-service/src/saga/handlers/order-events.handler';
import { SagaLedgerEventsConsumer } from '../../../apps/order-service/src/saga/handlers/ledger-events.handler';
import { SagaRiskEventsConsumer } from '../../../apps/order-service/src/saga/handlers/risk-events.handler';
import { SagaBalanceEventsConsumer } from '../../../apps/order-service/src/saga/handlers/balance-events.handler';
import { RetryEventsConsumer } from '../../../apps/order-service/src/saga/recovery/retry-events.consumer';
import { OrderOutboxRelay } from '../../../apps/order-service/src/saga/recovery/order-outbox.relay';

// Custom Kafkajs Mocking Boundaries to capture all Saga event consumers
const topicHandlers = new Map<string, any>();
const groupHandlers = new Map<string, any>();
const sentMessages: { topic: string; key: string; messages: any[] }[] = [];

jest.mock('kafkajs', () => {
  return {
    CompressionTypes: {
      None: 0,
      GZIP: 1,
      Snappy: 2,
      LZ4: 3,
      ZSTD: 4,
    },
    Kafka: jest.fn().mockImplementation(() => {
      let currentTopic = '';
      let currentGroupId = '';
      return {
        producer: jest.fn().mockImplementation(() => {
          return {
            connect: jest.fn().mockResolvedValue(undefined),
            send: jest.fn().mockImplementation(async (payload) => {
              sentMessages.push({
                topic: payload.topic,
                key: payload.key,
                messages: payload.messages.map((m: any) => JSON.parse(m.value.toString())),
              });
              return [{ topicName: payload.topic, partition: 0, offset: '0' }];
            }),
            disconnect: jest.fn().mockResolvedValue(undefined),
          };
        }),
        consumer: jest.fn().mockImplementation((config) => {
          currentGroupId = config.groupId;
          return {
            connect: jest.fn().mockResolvedValue(undefined),
            subscribe: jest.fn().mockImplementation(async (sub) => {
              currentTopic = sub.topic;
            }),
            run: jest.fn().mockImplementation(async (options) => {
              const topic = currentTopic;
              const groupId = currentGroupId;
              topicHandlers.set(topic, options.eachMessage);
              groupHandlers.set(groupId, options.eachMessage);
            }),
            disconnect: jest.fn().mockResolvedValue(undefined),
            commitOffsets: jest.fn().mockResolvedValue(undefined),
          };
        }),
      };
    }),
  };
});

describe('Saga Orchestrator Integration Tests', () => {
  let moduleFixture: TestingModule;
  let prisma: PrismaService;
  let sagaService: SagaService;
  let sagaRepository: SagaRepository;
  let timeoutScanner: SagaTimeoutScanner;
  let recoveryService: SagaRecoveryService;
  let inboxRepository: OrderInboxRepository;
  let orderOutboxRelay: OrderOutboxRelay;

  beforeAll(async () => {
    // Re-verify and clean process DATABASE_URL
    const originalUrl = process.env.DATABASE_URL;
    if (originalUrl) {
      const url = new URL(originalUrl);
      url.searchParams.delete('schema');
      process.env.DATABASE_URL = url.toString();
    }

    // Spy/Mock on SagaTimeoutScanner prototype onApplicationBootstrap to prevent background ticks
    jest.spyOn(SagaTimeoutScanner.prototype, 'onApplicationBootstrap').mockImplementation(async () => {
      // No-op to prevent background scanner loop during tests
    });

    // Spy/Mock on OrderOutboxRelay prototype onApplicationBootstrap to prevent background ticks
    jest.spyOn(OrderOutboxRelay.prototype, 'onApplicationBootstrap').mockImplementation(async () => {
      // No-op to prevent background relay loop during tests
    });

    try {
      moduleFixture = await Test.createTestingModule({
        imports: [AppModule],
      }).compile();
      await moduleFixture.init();
    } finally {
      process.env.DATABASE_URL = originalUrl;
    }

    prisma = moduleFixture.get<PrismaService>(PrismaService);
    sagaService = moduleFixture.get<SagaService>(SagaService);
    sagaRepository = moduleFixture.get<SagaRepository>(SagaRepository);
    timeoutScanner = moduleFixture.get<SagaTimeoutScanner>(SagaTimeoutScanner);
    recoveryService = moduleFixture.get<SagaRecoveryService>(SagaRecoveryService);
    inboxRepository = moduleFixture.get<OrderInboxRepository>(OrderInboxRepository);
    orderOutboxRelay = moduleFixture.get<OrderOutboxRelay>(OrderOutboxRelay);
  });

  afterAll(async () => {
    await prisma.client.$disconnect();
    await moduleFixture.close();
  });

  beforeEach(async () => {
    jest.clearAllMocks();
    sentMessages.length = 0;

    // Truncate order service schema tables in correct sequence
    await prisma.client.$executeRawUnsafe('TRUNCATE TABLE "order"."SagaTransition" CASCADE;');
    await prisma.client.$executeRawUnsafe('TRUNCATE TABLE "order"."SagaInstance" CASCADE;');
    await prisma.client.$executeRawUnsafe('TRUNCATE TABLE "order"."InboxEvent" CASCADE;');
    await prisma.client.$executeRawUnsafe('TRUNCATE TABLE "order"."Order" CASCADE;');
  });

  // Local helper to deliver mocked Kafka events directly to consumers
  async function deliverEvent(topic: string, envelope: any) {
    const handler = topicHandlers.get(topic);
    if (!handler) {
      throw new Error(`No consumer handler registered for topic: ${topic}`);
    }
    await handler({
      topic,
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(envelope)),
        offset: '1',
      },
    });
  }

  // Helper entity builder avoiding positional arguments typescript errors
  function makeSagaEntity(fields: Partial<SagaInstanceEntity> & { paymentId: string; correlationId: string }): SagaInstanceEntity {
    const id = fields.correlationId;
    const paymentId = fields.paymentId;
    const correlationId = fields.correlationId;
    const status = fields.status ?? SagaStatus.LEDGER_PENDING;
    const orderValidationStatus = fields.orderValidationStatus ?? OrderValidationStatus.PENDING;
    const merchantId = fields.merchantId ?? randomUUID();
    const amount = fields.amount ?? 1000;
    const currency = fields.currency ?? 'USD';
    const version = fields.version ?? 1;
    const startedAt = fields.startedAt ?? new Date();
    const completedAt = fields.completedAt ?? null;
    const createdAt = fields.createdAt ?? new Date();
    const updatedAt = fields.updatedAt ?? new Date();
    const failureReason = fields.failureReason ?? null;
    const failedAt = fields.failedAt ?? null;
    const originService = fields.originService ?? null;
    const stateUpdatedAt = fields.stateUpdatedAt ?? new Date();
    const retryCount = fields.retryCount ?? 0;
    const lastRetryAt = fields.lastRetryAt ?? null;
    const nextRetryAt = fields.nextRetryAt ?? null;
    const currentCommandId = fields.currentCommandId ?? null;
    const retryHandoffAt = fields.retryHandoffAt ?? null;
    const recoveredAt = fields.recoveredAt ?? null;
    const recoveryCount = fields.recoveryCount ?? 0;
    const recoveryReason = fields.recoveryReason ?? null;

    return new SagaInstanceEntity(
      id,
      paymentId,
      correlationId,
      status,
      orderValidationStatus,
      merchantId,
      amount,
      currency,
      version,
      startedAt,
      completedAt,
      createdAt,
      updatedAt,
      failureReason,
      failedAt,
      originService,
      stateUpdatedAt,
      retryCount,
      lastRetryAt,
      nextRetryAt,
      currentCommandId,
      retryHandoffAt,
      recoveredAt,
      recoveryCount,
      recoveryReason
    );
  }

  // Envelope factories
  function makePaymentCompleted(paymentId: string, correlationId: string, orderId: string) {
    return {
      eventId: randomUUID(),
      eventType: PAYMENT_COMPLETED,
      correlationId,
      causationId: correlationId,
      sagaId: randomUUID(),
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId,
        orderId,
        amount: 1500,
        currency: 'USD',
        merchantId: randomUUID(),
        paymentMethod: 'card',
        processorTransactionId: randomUUID(),
        completedAt: new Date().toISOString(),
      },
    };
  }

  function makeLedgerRecorded(sagaId: string, correlationId: string, causationId: string, paymentId: string) {
    return {
      eventId: randomUUID(),
      eventType: LEDGER_ENTRY_RECORDED,
      correlationId,
      causationId,
      sagaId,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId,
        entryId: randomUUID(),
      },
    };
  }

  function makeEligibilityApproved(sagaId: string, correlationId: string, causationId: string, paymentId?: string) {
    return {
      eventId: randomUUID(),
      eventType: ELIGIBILITY_APPROVED,
      correlationId,
      causationId,
      sagaId,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId: paymentId || randomUUID(),
        eligible: true,
      },
    };
  }

  function makeBalanceReserved(sagaId: string, correlationId: string, causationId: string, paymentId: string) {
    return {
      eventId: randomUUID(),
      eventType: BALANCE_RESERVED,
      correlationId,
      causationId,
      sagaId,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId,
      },
    };
  }

  // -------------------------------------------------------------
  // Test Case 1: Current Implemented Forward Flow to BALANCE_RESERVED
  // -------------------------------------------------------------
  it('Scenario 1: Current Implemented Forward Flow to BALANCE_RESERVED', async () => {
    const paymentId = randomUUID();
    const orderId = randomUUID();
    const correlationId = randomUUID();

    // 1. Deliver PaymentCompleted to start Saga
    const pc = makePaymentCompleted(paymentId, correlationId, orderId);
    await deliverEvent('payments.completed', pc);

    // Verify Saga created in LEDGER_PENDING
    const saga = await sagaRepository.findByPaymentId(paymentId);
    expect(saga).toBeDefined();
    expect(saga!.status).toBe(SagaStatus.LEDGER_PENDING);
    expect(saga!.correlationId).toBe(correlationId);

    // Find the CheckOrderEligibility and RecordLedgerEntry commands
    expect(sentMessages).toHaveLength(2);
    const orderCmd = sentMessages.find(m => m.topic === 'order.commands');
    const ledgerCmd = sentMessages.find(m => m.topic === 'ledger.commands');
    expect(orderCmd).toBeDefined();
    expect(ledgerCmd).toBeDefined();
    const commandIdOrder = orderCmd!.messages[0].eventId;
    const commandIdLedger = ledgerCmd!.messages[0].eventId;

    // 2. Deliver OrderEligibilityConfirmed to confirm order validation
    sentMessages.length = 0;
    const oec = {
      eventId: randomUUID(),
      eventType: ORDER_ELIGIBILITY_CONFIRMED,
      correlationId,
      causationId: commandIdOrder,
      sagaId: saga!.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        orderId,
      },
    };
    await deliverEvent('order.events', oec);

    // Verify order validation status transitions to CONFIRMED, Saga remains in LEDGER_PENDING
    const sagaOec = await sagaRepository.findById(saga!.id);
    expect(sagaOec!.orderValidationStatus).toBe(OrderValidationStatus.CONFIRMED);
    expect(sagaOec!.status).toBe(SagaStatus.LEDGER_PENDING);

    // 3. Deliver LedgerEntryRecorded
    sentMessages.length = 0;
    const lr = makeLedgerRecorded(saga!.id, correlationId, commandIdLedger, paymentId);
    await deliverEvent('ledger.events', lr);

    // Verify Saga advanced to ELIGIBILITY_PENDING and CheckPayoutEligibility sent
    const saga2 = await sagaRepository.findById(saga!.id);
    expect(saga2!.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
    expect(sentMessages).toHaveLength(1);
    expect(sentMessages[0]?.topic).toBe('risk.commands');
    expect(sentMessages[0]?.messages?.[0]?.eventType).toBe(CHECK_PAYOUT_ELIGIBILITY);
    const commandId2 = sentMessages[0]?.messages?.[0]?.eventId;

    // 4. Deliver EligibilityApproved
    sentMessages.length = 0;
    const ea = makeEligibilityApproved(saga!.id, correlationId, commandId2, paymentId);
    await deliverEvent('risk.events', ea);

    // Verify Saga advanced to BALANCE_PENDING and ReserveBalance command sent
    const saga3 = await sagaRepository.findById(saga!.id);
    expect(saga3!.status).toBe(SagaStatus.BALANCE_PENDING);
    expect(sentMessages).toHaveLength(1);
    expect(sentMessages[0]?.topic).toBe('balance.commands');
    expect(sentMessages[0]?.messages?.[0]?.eventType).toBe(RESERVE_BALANCE);
    const commandId3 = sentMessages[0]?.messages?.[0]?.eventId;

    // 5. Deliver BalanceReserved
    sentMessages.length = 0;
    const br = makeBalanceReserved(saga!.id, correlationId, commandId3, paymentId);
    await deliverEvent('balance.events', br);

    // Verify Saga reaches BALANCE_RESERVED (forward boundary)
    const saga4 = await sagaRepository.findById(saga!.id);
    expect(saga4!.status).toBe(SagaStatus.BALANCE_RESERVED);
    expect(saga4!.failureReason).toBeNull();
    expect(sentMessages).toHaveLength(0); // No downstream command expected at the boundary
  });

  // -------------------------------------------------------------
  // Test Case 2: Ledger Timeout and Retry
  // -------------------------------------------------------------
  it('Scenario 2: should retry a timed-out ledger command before starting compensation', async () => {
    const paymentId = randomUUID();
    const orderId = randomUUID();
    const correlationId = randomUUID();

    const pc = makePaymentCompleted(paymentId, correlationId, orderId);
    await deliverEvent('payments.completed', pc);

    const saga = await sagaRepository.findByPaymentId(paymentId);
    const orderCmd = sentMessages.find(m => m.topic === 'order.commands');
    const ledgerCmd = sentMessages.find(m => m.topic === 'ledger.commands');
    const cmdIdOrder = orderCmd!.messages[0].eventId;
    const cmdId1 = ledgerCmd!.messages[0].eventId;

    // Confirm order validation first
    const oec = {
      eventId: randomUUID(),
      eventType: ORDER_ELIGIBILITY_CONFIRMED,
      correlationId,
      causationId: cmdIdOrder,
      sagaId: saga!.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        orderId,
      },
    };
    await deliverEvent('order.events', oec);

    // Fast-forward last execution timestamp to trigger timeout
    const pastDate = new Date(Date.now() - 300000); // 5 mins ago (threshold is 60s)
    await prisma.client.sagaInstance.update({
      where: { id: saga!.id },
      data: { stateUpdatedAt: pastDate },
    });



    sentMessages.length = 0;

    // Run timeout scanner
    await (timeoutScanner as any).scanForTimeouts();

    // Trigger Outbox Relay manually to publish SCHEDULE_RETRY
    await (orderOutboxRelay as any).processPendingEvents();

    // Verify handoff to Retry Scheduler
    const sagaScanned = await sagaRepository.findById(saga!.id);
    expect(sagaScanned!.retryHandoffAt).toBeDefined();
    expect(sentMessages).toHaveLength(1);
    expect(sentMessages[0]?.topic).toBe('retry.commands');
    expect(sentMessages[0]?.messages?.[0]?.sagaId).toBe(saga!.id);

    // Deliver SagaRetryRegistered (retry registered)
    const retryReg = {
      eventId: randomUUID(),
      eventType: SAGA_RETRY_REGISTERED,
      correlationId,
      causationId: sentMessages[0]?.messages?.[0]?.eventId,
      sagaId: saga!.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        originalEventId: cmdId1,
        attempt: 1,
        nextExecutionTime: new Date(Date.now() + 1000).toISOString(),
      },
    };
    await deliverEvent('retry.events', retryReg);

    const sagaRetried = await sagaRepository.findById(saga!.id);
    expect(sagaRetried!.retryCount).toBe(1);
    expect(sagaRetried!.retryHandoffAt).toBeNull();
    expect(sagaRetried!.nextRetryAt).toBeDefined();

    // Deliver LedgerEntryRecorded -> resumes forward
    const lr = makeLedgerRecorded(saga!.id, correlationId, cmdId1, paymentId);
    await deliverEvent('ledger.events', lr);

    const sagaFinal = await sagaRepository.findById(saga!.id);
    expect(sagaFinal!.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
    expect(sagaFinal!.retryCount).toBe(0);
  });

  // -------------------------------------------------------------
  // Test Case 3: Retry Exhaustion and Failure Metadata
  // -------------------------------------------------------------
  it('Scenario 3: should record retry exhaustion metadata correctly in LEDGER_PENDING', async () => {
    const paymentId = randomUUID();
    const orderId = randomUUID();
    const correlationId = randomUUID();

    const pc = makePaymentCompleted(paymentId, correlationId, orderId);
    await deliverEvent('payments.completed', pc);

    const saga = await sagaRepository.findByPaymentId(paymentId);
    const ledgerCmd = sentMessages.find(m => m.topic === 'ledger.commands');
    const cmdId1 = ledgerCmd!.messages[0].eventId;

    // Seed retry Count to 3 (exhausted)
    await prisma.client.sagaInstance.update({
      where: { id: saga!.id },
      data: { retryCount: 3 },
    });

    sentMessages.length = 0;

    // Deliver SagaStepExecutionFailed
    const failureEvent = {
      eventId: randomUUID(),
      eventType: SAGA_STEP_EXECUTION_FAILED,
      correlationId,
      causationId: randomUUID(),
      sagaId: saga!.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        originalEventId: cmdId1,
        failureReason: 'Permanent ledger timeout failure',
      },
    };
    await deliverEvent('retry.events', failureEvent);

    // Verify Saga updates failure metadata but remains in LEDGER_PENDING with scenario NONE
    const sagaFailed = await sagaRepository.findById(saga!.id);
    expect(sagaFailed!.failureReason).toBe('Permanent ledger timeout failure');
    expect(sagaFailed!.status).toBe(SagaStatus.LEDGER_PENDING);
    expect(sentMessages).toHaveLength(0); // No rollback commands sent
  });

  // -------------------------------------------------------------
  // Test Case 4: Reverse-Order Compensation
  // -------------------------------------------------------------
  it('Scenario 4: should dispatch ReverseBalance then ReverseLedgerEntry in reverse order', async () => {
    const paymentId = randomUUID();
    const correlationId = randomUUID();

    // Setup Saga at BALANCE_RESERVED
    const saga = await sagaRepository.create(
      makeSagaEntity({
        paymentId,
        correlationId: randomUUID(),
        status: SagaStatus.BALANCE_RESERVED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
        version: 1,
        currentCommandId: randomUUID(),
      })
    );

    // Deliver SagaStepExecutionFailed to trigger compensation
    const failureEvent = {
      eventId: randomUUID(),
      eventType: SAGA_STEP_EXECUTION_FAILED,
      correlationId,
      causationId: randomUUID(),
      sagaId: saga!.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        originalEventId: saga!.currentCommandId!,
        failureReason: 'Compensating post-reservation workflow failure',
      },
    };
    await deliverEvent('retry.events', failureEvent);

    // Verify ReverseBalance is dispatched first and checkpoint BALANCE_REVERSAL_DISPATCHED saved
    expect(sentMessages).toHaveLength(1);
    expect(sentMessages[0]?.topic).toBe('balance.commands');
    expect(sentMessages[0]?.messages?.[0]?.eventType).toBe(REVERSE_BALANCE);
    const balanceRevCommandId = sentMessages[0]?.messages?.[0]?.eventId;

    const checkpoint1 = await prisma.client.sagaTransition.findFirst({
      where: {
        sagaId: saga.id,
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        toState: 'BALANCE_REVERSAL_DISPATCHED',
      },
    });
    expect(checkpoint1).toBeDefined();

    // Deliver BalanceReversed ack
    sentMessages.length = 0;
    const balanceReversedAck = {
      eventId: randomUUID(),
      eventType: BALANCE_REVERSED,
      correlationId,
      causationId: balanceRevCommandId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId,
      },
    };
    await deliverEvent('balance.events', balanceReversedAck);

    // Verify ReverseLedgerEntry is now dispatched, checkpoint BALANCE_REVERSAL_ACKNOWLEDGED and LEDGER_REVERSAL_DISPATCHED hit
    expect(sentMessages).toHaveLength(1);
    expect(sentMessages[0]?.topic).toBe('ledger.commands');
    expect(sentMessages[0]?.messages?.[0]?.eventType).toBe(REVERSE_LEDGER_ENTRY);
    const ledgerRevCommandId = sentMessages[0]?.messages?.[0]?.eventId;

    const checkpoint2 = await prisma.client.sagaTransition.findFirst({
      where: {
        sagaId: saga.id,
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        toState: 'BALANCE_REVERSAL_ACKNOWLEDGED',
      },
    });
    expect(checkpoint2).toBeDefined();

    const checkpoint3 = await prisma.client.sagaTransition.findFirst({
      where: {
        sagaId: saga.id,
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        toState: 'LEDGER_REVERSAL_DISPATCHED',
      },
    });
    expect(checkpoint3).toBeDefined();

    // Deliver LedgerReversed ack
    sentMessages.length = 0;
    const ledgerReversedAck = {
      eventId: randomUUID(),
      eventType: LEDGER_REVERSED,
      correlationId,
      causationId: ledgerRevCommandId,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId,
        originalEntryId: randomUUID(),
      },
    };
    await deliverEvent('ledger.events', ledgerReversedAck);

    // Verify Saga reaches terminal CLOSED (status is CLOSED, and transition to CLOSED exists)
    const finalSaga = await sagaRepository.findById(saga.id);
    expect(finalSaga!.status).toBe(SagaStatus.CLOSED);

    const closedTransition = await prisma.client.sagaTransition.findFirst({
      where: {
        sagaId: saga.id,
        transitionType: SagaTransitionType.SAGA_STATUS,
        toState: SagaStatus.CLOSED,
      },
    });
    expect(closedTransition).toBeDefined();
  });

  // -------------------------------------------------------------
  // Test Case 5: Eligibility Denied Compensation
  // -------------------------------------------------------------
  it('Scenario 5: should reverse only the ledger when eligibility is denied', async () => {
    const paymentId = randomUUID();
    const orderId = randomUUID();
    const correlationId = randomUUID();

    const pc = makePaymentCompleted(paymentId, correlationId, orderId);
    await deliverEvent('payments.completed', pc);

    const saga = await sagaRepository.findByPaymentId(paymentId);
    const orderCmd = sentMessages.find(m => m.topic === 'order.commands');
    const ledgerCmd = sentMessages.find(m => m.topic === 'ledger.commands');
    const cmdIdOrder = orderCmd!.messages[0].eventId;
    const cmdId1 = ledgerCmd!.messages[0].eventId;

    // Confirm order validation first
    const oec = {
      eventId: randomUUID(),
      eventType: ORDER_ELIGIBILITY_CONFIRMED,
      correlationId,
      causationId: cmdIdOrder,
      sagaId: saga!.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        orderId,
      },
    };
    await deliverEvent('order.events', oec);

    // Deliver LedgerEntryRecorded to move to ELIGIBILITY_PENDING
    sentMessages.length = 0;
    const lr = makeLedgerRecorded(saga!.id, correlationId, cmdId1, paymentId);
    await deliverEvent('ledger.events', lr);

    const saga2 = await sagaRepository.findById(saga!.id);
    const cmdId2 = sentMessages[0]?.messages?.[0]?.eventId;

    // Deliver EligibilityDenied
    sentMessages.length = 0;
    const eligibilityDenied = {
      eventId: randomUUID(),
      eventType: ELIGIBILITY_DENIED,
      correlationId,
      causationId: cmdId2,
      sagaId: saga!.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId,
        reason: 'Merchant payout blacklist check failed',
      },
    };
    await deliverEvent('risk.events', eligibilityDenied);

    // Verify only ReverseLedgerEntry is dispatched (ReverseBalance is skipped)
    expect(sentMessages).toHaveLength(1);
    expect(sentMessages[0]?.topic).toBe('ledger.commands');
    expect(sentMessages[0]?.messages?.[0]?.eventType).toBe(REVERSE_LEDGER_ENTRY);

    // Verify checkpoint LEDGER_REVERSAL_DISPATCHED is written directly
    const cp = await prisma.client.sagaTransition.findFirst({
      where: {
        sagaId: saga!.id,
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        toState: 'LEDGER_REVERSAL_DISPATCHED',
      },
    });
    expect(cp).toBeDefined();

    const cpBalance = await prisma.client.sagaTransition.findFirst({
      where: {
        sagaId: saga!.id,
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        toState: 'BALANCE_REVERSAL_DISPATCHED',
      },
    });
    expect(cpBalance).toBeNull();
  });

  // -------------------------------------------------------------
  // Test Case 6: Balance Failure Compensation
  // -------------------------------------------------------------
  it('Scenario 6: should reverse the ledger when balance reservation fails permanently', async () => {
    const paymentId = randomUUID();
    const correlationId = randomUUID();

    // Setup Saga at BALANCE_PENDING
    const saga = await sagaRepository.create(
      makeSagaEntity({
        paymentId,
        correlationId: randomUUID(),
        status: SagaStatus.BALANCE_PENDING,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
        version: 1,
        currentCommandId: randomUUID(),
      })
    );

    // Deliver BalanceReservationFailed
    const balanceFailed = {
      eventId: randomUUID(),
      eventType: BALANCE_RESERVATION_FAILED,
      correlationId,
      causationId: saga.currentCommandId!,
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId,
        reason: 'Insufficient merchant balance',
      },
    };
    await deliverEvent('balance.events', balanceFailed);

    // Verify only ReverseLedgerEntry is dispatched
    expect(sentMessages).toHaveLength(1);
    expect(sentMessages[0]?.topic).toBe('ledger.commands');
    expect(sentMessages[0]?.messages?.[0]?.eventType).toBe(REVERSE_LEDGER_ENTRY);

    const cpLedger = await prisma.client.sagaTransition.findFirst({
      where: {
        sagaId: saga.id,
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        toState: 'LEDGER_REVERSAL_DISPATCHED',
      },
    });
    expect(cpLedger).toBeDefined();

    const cpBalance = await prisma.client.sagaTransition.findFirst({
      where: {
        sagaId: saga.id,
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        toState: 'BALANCE_REVERSAL_DISPATCHED',
      },
    });
    expect(cpBalance).toBeNull();
  });

  // -------------------------------------------------------------
  // Test Case 7: Failure After Balance Reserved
  // -------------------------------------------------------------
  it('Scenario 7: should reverse balance before ledger after a post-reservation failure', async () => {
    const paymentId = randomUUID();
    const correlationId = randomUUID();

    // Setup Saga at BALANCE_RESERVED
    const saga = await sagaRepository.create(
      makeSagaEntity({
        paymentId,
        correlationId,
        status: SagaStatus.BALANCE_RESERVED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
        version: 1,
        currentCommandId: randomUUID(),
      })
    );

    // Deliver a generic step failure (or BalanceReservationFailed / similar trigger) after reserve
    const failureEvent = {
      eventId: randomUUID(),
      eventType: SAGA_STEP_EXECUTION_FAILED,
      correlationId,
      causationId: randomUUID(),
      sagaId: saga.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        originalEventId: saga.currentCommandId!,
        failureReason: 'Simulating failure after reservation was successful',
      },
    };
    await deliverEvent('retry.events', failureEvent);

    // Verify reverse balance is dispatched
    const relevantMessages = sentMessages.filter(
      (m) => m.messages?.[0]?.correlationId === correlationId || m.messages?.[0]?.sagaId === saga.id
    );
    expect(relevantMessages).toHaveLength(1);
    expect(relevantMessages[0]?.topic).toBe('balance.commands');
    expect(relevantMessages[0]?.messages?.[0]?.eventType).toBe(REVERSE_BALANCE);
  });

  // -------------------------------------------------------------
  // Test Case 8: Crash Recovery and Resumption
  // -------------------------------------------------------------
  it('Scenario 8: should resume a persisted forward saga and redispatch compensating command after orchestrator restart', async () => {
    // 1. Seed Saga A at forward LEDGER_RECORDED
    const sagaA = await sagaRepository.create(
      makeSagaEntity({
        paymentId: randomUUID(),
        correlationId: randomUUID(),
        status: SagaStatus.LEDGER_RECORDED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
        version: 1,
        currentCommandId: randomUUID(),
      })
    );

    // 2. Seed Saga B in compensation with BALANCE_REVERSAL_DISPATCHED checkpoint written
    const sagaB = await sagaRepository.create(
      makeSagaEntity({
        paymentId: randomUUID(),
        correlationId: randomUUID(),
        status: SagaStatus.BALANCE_RESERVED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
        version: 1,
        currentCommandId: randomUUID(),
        failureReason: 'Failure triggers rollback',
        failedAt: new Date(),
        originService: 'risk-engine',
      })
    );
    const balanceReversalCommandId = randomUUID();
    await prisma.client.sagaTransition.create({
      data: {
        sagaId: sagaB.id,
        correlationId: randomUUID(),
        transitionType: SagaTransitionType.COMPENSATION_STEP,
        fromState: SagaStatus.BALANCE_RESERVED,
        toState: 'BALANCE_REVERSAL_DISPATCHED',
        eventId: balanceReversalCommandId,
        causationId: randomUUID(),
        eventType: REVERSE_BALANCE,
      },
    });

    sentMessages.length = 0;

    // Trigger recovery
    await recoveryService.recoverIncompleteSagas();

    // Verify Saga A is updated to ELIGIBILITY_PENDING and CheckPayoutEligibility is dispatched
    const recoveredA = await sagaRepository.findById(sagaA.id);
    expect(recoveredA!.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
    expect(recoveredA!.recoveryCount).toBe(1);

    const checkPayoutMsg = sentMessages.find(m => m.topic === 'risk.commands');
    expect(checkPayoutMsg).toBeDefined();
    expect(checkPayoutMsg!.messages[0].eventType).toBe(CHECK_PAYOUT_ELIGIBILITY);

    // Verify Saga B is redispatched using the exact balanceReversalCommandId from the checkpoint
    const recoveredB = await sagaRepository.findById(sagaB.id);
    expect(recoveredB!.recoveryCount).toBe(1);

    const reverseBalanceMsg = sentMessages.find(m => m.topic === 'balance.commands');
    expect(reverseBalanceMsg).toBeDefined();
    expect(reverseBalanceMsg!.messages[0].eventType).toBe(REVERSE_BALANCE);
    expect(reverseBalanceMsg!.messages[0].eventId).toBe(balanceReversalCommandId);
  });

  // -------------------------------------------------------------
  // Test Case 9: Duplicate Event Delivery
  // -------------------------------------------------------------
  it('Scenario 9: should ignore duplicate events already recorded in the inbox', async () => {
    const paymentId = randomUUID();
    const orderId = randomUUID();
    const correlationId = randomUUID();

    const pc = makePaymentCompleted(paymentId, correlationId, orderId);
    await deliverEvent('payments.completed', pc);

    const saga = await sagaRepository.findByPaymentId(paymentId);
    const orderCmd = sentMessages.find(m => m.topic === 'order.commands');
    const ledgerCmd = sentMessages.find(m => m.topic === 'ledger.commands');
    const cmdIdOrder = orderCmd!.messages[0].eventId;
    const cmdId1 = ledgerCmd!.messages[0].eventId;

    // Confirm order validation first
    const oec = {
      eventId: randomUUID(),
      eventType: ORDER_ELIGIBILITY_CONFIRMED,
      correlationId,
      causationId: cmdIdOrder,
      sagaId: saga!.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        orderId,
      },
    };
    await deliverEvent('order.events', oec);

    // Deliver LedgerEntryRecorded (first time)
    sentMessages.length = 0;
    const eventId = randomUUID();
    const lr = {
      eventId,
      eventType: LEDGER_ENTRY_RECORDED,
      correlationId,
      causationId: cmdId1,
      sagaId: saga!.id,
      timestamp: new Date().toISOString(),
      version: 1,
      payload: {
        paymentId,
        entryId: randomUUID(),
      },
    };
    await deliverEvent('ledger.events', lr);

    // Verify transitions to ELIGIBILITY_PENDING and sends CheckPayoutEligibility
    const saga2 = await sagaRepository.findById(saga!.id);
    expect(saga2!.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
    expect(sentMessages).toHaveLength(1);

    const inboxCount1 = await prisma.client.inboxEvent.count();
    expect(inboxCount1).toBe(3); // PaymentCompleted + OrderEligibilityConfirmed + LedgerEntryRecorded

    // Deliver same LedgerEntryRecorded (second time with duplicate eventId)
    sentMessages.length = 0;
    await deliverEvent('ledger.events', lr);

    // Verify Saga state does not transition again, no second command is emitted, no extra Inbox record created
    const saga3 = await sagaRepository.findById(saga!.id);
    expect(saga3!.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
    expect(sentMessages).toHaveLength(0);

    const inboxCount2 = await prisma.client.inboxEvent.count();
    expect(inboxCount2).toBe(3); // Unchanged!
  });

  // -------------------------------------------------------------
  // Test Case 10: Duplicate Saga Initiation
  // -------------------------------------------------------------
  it('Scenario 10: should create only one saga for duplicate initiating event delivery', async () => {
    const paymentId = randomUUID();
    const orderId = randomUUID();
    const correlationId = randomUUID();

    const pc = makePaymentCompleted(paymentId, correlationId, orderId);

    // Deliver first initiation
    await deliverEvent('payments.completed', pc);

    const sagaCount1 = await prisma.client.sagaInstance.count();
    expect(sagaCount1).toBe(1);
    expect(sentMessages).toHaveLength(2);

    // Deliver duplicate initiation
    sentMessages.length = 0;
    await deliverEvent('payments.completed', pc);

    // Verify only one Saga exists and no new command was dispatched
    const sagaCount2 = await prisma.client.sagaInstance.count();
    expect(sagaCount2).toBe(1);
    expect(sentMessages).toHaveLength(0);

    const inboxCount = await prisma.client.inboxEvent.count();
    expect(inboxCount).toBe(1); // One Inbox record, no duplicate created
  });

  // -------------------------------------------------------------
  // Test Case 11: Duplicate Acks
  // -------------------------------------------------------------
  describe('Scenario 11: Duplicate Acknowledgements', () => {
    it('Check A: should not dispatch the next command twice for duplicate LedgerEntryRecorded', async () => {
      const paymentId = randomUUID();
      const correlationId = randomUUID();

      // Seed Saga at ELIGIBILITY_PENDING with transition already persisted
      const saga = await sagaRepository.create(
        makeSagaEntity({
          paymentId,
          correlationId: randomUUID(),
          status: SagaStatus.ELIGIBILITY_PENDING,
          orderValidationStatus: OrderValidationStatus.CONFIRMED,
          version: 2,
          currentCommandId: randomUUID(),
        })
      );
      const ackEventId = randomUUID();
      await prisma.client.sagaTransition.create({
        data: {
          sagaId: saga.id,
          correlationId,
          transitionType: SagaTransitionType.SAGA_STATUS,
          fromState: SagaStatus.LEDGER_PENDING,
          toState: SagaStatus.LEDGER_RECORDED,
          eventId: ackEventId,
          causationId: randomUUID(),
          eventType: LEDGER_ENTRY_RECORDED,
        },
      });

      // Write duplicate event into Inbox to match real deduplication check
      await prisma.client.inboxEvent.create({
        data: {
          eventId: ackEventId,
          consumer: 'order-service-saga-ledger-events',
          status: 'PROCESSED',
          eventType: LEDGER_ENTRY_RECORDED,
          correlationId,
          causationId: randomUUID(),
          sagaId: saga.id,
          timestamp: new Date(),
          version: 1,
          payload: {},
        },
      });

      sentMessages.length = 0;

      // Deliver duplicate LedgerEntryRecorded
      const duplicateAck = {
        eventId: ackEventId,
        eventType: LEDGER_ENTRY_RECORDED,
        correlationId,
        causationId: randomUUID(),
        sagaId: saga.id,
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId,
          entryId: randomUUID(),
        },
      };
      await deliverEvent('ledger.events', duplicateAck);

      // Verify no changes to status and no commands dispatched
      const sagaCheck = await sagaRepository.findById(saga.id);
      expect(sagaCheck!.status).toBe(SagaStatus.ELIGIBILITY_PENDING);
      expect(sentMessages).toHaveLength(0);
    });

    it('Check B: should not transition saga state twice for duplicate BalanceReserved', async () => {
      const paymentId = randomUUID();
      const correlationId = randomUUID();

      // Seed Saga at BALANCE_RESERVED with transition already persisted
      const saga = await sagaRepository.create(
        makeSagaEntity({
          paymentId,
          correlationId: randomUUID(),
          status: SagaStatus.BALANCE_RESERVED,
          orderValidationStatus: OrderValidationStatus.CONFIRMED,
          version: 2,
          currentCommandId: null,
        })
      );
      const ackEventId = randomUUID();
      await prisma.client.sagaTransition.create({
        data: {
          sagaId: saga.id,
          correlationId,
          transitionType: SagaTransitionType.SAGA_STATUS,
          fromState: SagaStatus.BALANCE_PENDING,
          toState: SagaStatus.BALANCE_RESERVED,
          eventId: ackEventId,
          causationId: randomUUID(),
          eventType: BALANCE_RESERVED,
        },
      });

      // Write duplicate event into Inbox
      await prisma.client.inboxEvent.create({
        data: {
          eventId: ackEventId,
          consumer: 'order-service-saga-balance-events',
          status: 'PROCESSED',
          eventType: BALANCE_RESERVED,
          correlationId,
          causationId: randomUUID(),
          sagaId: saga.id,
          timestamp: new Date(),
          version: 1,
          payload: {},
        },
      });

      sentMessages.length = 0;

      // Deliver duplicate BalanceReserved
      const duplicateAck = {
        eventId: ackEventId,
        eventType: BALANCE_RESERVED,
        correlationId,
        causationId: randomUUID(),
        sagaId: saga.id,
        timestamp: new Date().toISOString(),
        version: 1,
        payload: {
          paymentId,
        },
      };
      await deliverEvent('balance.events', duplicateAck);

      // Verify no changes to status and no commands dispatched
      const sagaCheck = await sagaRepository.findById(saga.id);
      expect(sagaCheck!.status).toBe(SagaStatus.BALANCE_RESERVED);
      expect(sentMessages).toHaveLength(0);
    });
  });

  // -------------------------------------------------------------
  // Test Case 12: Terminal Saga Stability
  // -------------------------------------------------------------
  it('Scenario 12: should not recover or update a closed saga', async () => {
    // Seed a CLOSED (compensated) Saga
    const saga = await sagaRepository.create(
      makeSagaEntity({
        paymentId: randomUUID(),
        correlationId: randomUUID(),
        status: SagaStatus.CLOSED,
        orderValidationStatus: OrderValidationStatus.CONFIRMED,
        version: 1,
        failureReason: 'Compensated successfully',
        failedAt: new Date(),
        originService: 'risk-engine',
      })
    );

    // Deliver late duplicate ack LedgerEntryRecorded
    const ack = makeLedgerRecorded(saga.id, randomUUID(), randomUUID(), saga.paymentId);
    await deliverEvent('ledger.events', ack);

    // Verify Saga status remains CLOSED
    const finalSaga = await sagaRepository.findById(saga.id);
    expect(finalSaga!.status).toBe(SagaStatus.CLOSED);

    // Run recovery
    sentMessages.length = 0;
    await recoveryService.recoverIncompleteSagas();
    expect(sentMessages).toHaveLength(0); // CLOSED sagas are ignored by recovery scan
  });

  // -------------------------------------------------------------
  // Test Case 13: Transactional Rollback Verification
  // -------------------------------------------------------------
  it('Scenario 13: should rollback SagaInstance update if SagaTransition insertion fails inside transaction', async () => {
    const paymentId = randomUUID();
    const orderId = randomUUID();
    const correlationId = randomUUID();

    const pc = makePaymentCompleted(paymentId, correlationId, orderId);
    await deliverEvent('payments.completed', pc);

    const saga = await sagaRepository.findByPaymentId(paymentId);
    expect(saga!.status).toBe(SagaStatus.LEDGER_PENDING);
    expect(saga!.version).toBe(0);

    // Spy on $transaction to inject error inside sagaTransition.create call
    const originalTransaction = prisma.client.$transaction;
    jest.spyOn(prisma.client, '$transaction').mockImplementation(async (callback: any) => {
      return originalTransaction.call(prisma.client, async (tx: any) => {
        jest.spyOn(tx.sagaTransition, 'create').mockRejectedValueOnce(
          new Error('Forced transaction rollback database failure')
        );
        return callback(tx);
      });
    });

    const transitions = [
      {
        transitionType: SagaTransitionType.SAGA_STATUS,
        fromState: SagaStatus.LEDGER_PENDING,
        toState: SagaStatus.LEDGER_RECORDED,
        eventId: randomUUID(),
        causationId: randomUUID(),
        eventType: LEDGER_ENTRY_RECORDED,
      },
    ];

    saga!.status = SagaStatus.LEDGER_RECORDED;

    // Trigger update
    await expect(sagaRepository.update(saga!, transitions)).rejects.toThrow(
      'Forced transaction rollback database failure'
    );

    // Verify database state was fully rolled back
    const rolledBackSaga = await prisma.client.sagaInstance.findUnique({
      where: { id: saga!.id },
    });
    expect(rolledBackSaga!.status).toBe(SagaStatus.LEDGER_PENDING);
    expect(rolledBackSaga!.version).toBe(0);

    const transitionCount = await prisma.client.sagaTransition.count({
      where: { sagaId: saga!.id },
    });
    expect(transitionCount).toBe(2);
  });
});
