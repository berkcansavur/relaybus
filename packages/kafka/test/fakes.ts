import { vi } from 'vitest';
import type { EventEnvelope, RelayBusLogger } from '@relaybus/core';
import type {
  KafkaClientLike,
  KafkaConsumerEvent,
  KafkaConsumerEventNames,
  KafkaConsumerLike,
  KafkaEachMessagePayload,
  KafkaIncomingHeaders,
  KafkaProducerLike,
  KafkaProducerRecord,
  KafkaRunConfig,
} from '../src/index.js';
import { encodeEnvelope } from '../src/index.js';

export const EVENT_NAMES: Required<KafkaConsumerEventNames> = {
  GROUP_JOIN: 'join',
  CRASH: 'crash',
  REBALANCING: 'rebalancing',
  STOP: 'stop',
  DISCONNECT: 'disconnect',
};

export function createEnvelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: '0190a000-0000-7000-8000-0000000000aa',
    type: 'order.placed.v1',
    source: 'orders',
    key: 'order-1',
    payload: { orderId: '1' },
    headers: { traceparent: '00-abc-def-01' },
    correlationId: 'flow-1',
    causationId: 'cause-1',
    occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    ...overrides,
  };
}

export function createLogger(): RelayBusLogger & {
  log: ReturnType<typeof vi.fn<(message: string) => void>>;
  warn: ReturnType<typeof vi.fn<(message: string) => void>>;
  error: ReturnType<typeof vi.fn<(message: string) => void>>;
} {
  return {
    log: vi.fn<(message: string) => void>(),
    warn: vi.fn<(message: string) => void>(),
    error: vi.fn<(message: string) => void>(),
  };
}

export class FakeProducer implements KafkaProducerLike {
  readonly sent: KafkaProducerRecord[] = [];
  connectError: Error | undefined;
  sendError: Error | undefined;
  disconnectError: Error | undefined;
  connectCalls = 0;
  disconnected = false;

  async connect(): Promise<void> {
    this.connectCalls += 1;
    if (this.connectError !== undefined) {
      throw this.connectError;
    }
  }

  /** When set, `send` never settles on its own. */
  hang = false;

  async send(record: KafkaProducerRecord): Promise<unknown> {
    if (this.hang) {
      return new Promise<never>(() => undefined);
    }
    if (this.sendError !== undefined) {
      throw this.sendError;
    }
    this.sent.push(record);
    return [];
  }

  async disconnect(): Promise<void> {
    this.disconnected = true;
    if (this.disconnectError !== undefined) {
      throw this.disconnectError;
    }
  }
}

export class FakeConsumer implements KafkaConsumerLike {
  readonly listeners = new Map<string, (event: KafkaConsumerEvent) => void>();
  eachMessage: KafkaRunConfig['eachMessage'] | undefined;
  subscription: { topics: string[]; fromBeginning: boolean } | undefined;
  connectError: Error | undefined;
  subscribeError: Error | undefined;
  runError: Error | undefined;
  disconnectError: Error | undefined;
  disconnected = false;
  runCalls = 0;
  /** Event emitted from `run`, as a real client does while it joins. */
  emitOnRun: string | undefined = EVENT_NAMES.GROUP_JOIN;
  emitPayloadOnRun: unknown = undefined;
  events: KafkaConsumerEventNames | undefined = EVENT_NAMES;

  on(name: string, listener: (event: KafkaConsumerEvent) => void): void {
    this.listeners.set(name, listener);
  }

  async connect(): Promise<void> {
    if (this.connectError !== undefined) {
      throw this.connectError;
    }
  }

  async subscribe(subscription: { topics: string[]; fromBeginning: boolean }): Promise<void> {
    if (this.subscribeError !== undefined) {
      throw this.subscribeError;
    }
    this.subscription = subscription;
  }

  async run(config: KafkaRunConfig): Promise<void> {
    this.runCalls += 1;
    if (this.runError !== undefined) {
      throw this.runError;
    }
    this.eachMessage = config.eachMessage;
    if (this.emitOnRun !== undefined) {
      this.emit(this.emitOnRun, this.emitPayloadOnRun);
    }
  }

  async disconnect(): Promise<void> {
    this.disconnected = true;
    if (this.disconnectError !== undefined) {
      throw this.disconnectError;
    }
  }

  emit(name: string, payload?: unknown): void {
    this.listeners.get(name)?.({ payload });
  }

  async deliver(
    message: Partial<KafkaEachMessagePayload['message']>,
    heartbeat: () => Promise<void> = async () => undefined,
  ): Promise<void> {
    await this.eachMessage!({
      topic: 'orders',
      partition: 0,
      message: { key: null, value: null, offset: '7', ...message },
      heartbeat,
    });
  }
}

export class FakeKafka implements KafkaClientLike {
  readonly producer_ = new FakeProducer();
  readonly consumer_ = new FakeConsumer();
  producerConfig: unknown;
  consumerConfig: unknown;

  producer(config: unknown): KafkaProducerLike {
    this.producerConfig = config;
    return this.producer_;
  }

  consumer(config: unknown): KafkaConsumerLike {
    this.consumerConfig = config;
    return this.consumer_;
  }
}

/** The record a producer of this package would write for `envelope`, as a consumer sees it. */
export function asIncoming(envelope: EventEnvelope): {
  key: Buffer | null;
  value: Buffer;
  headers: KafkaIncomingHeaders;
  offset: string;
} {
  const encoded = encodeEnvelope(envelope);
  return {
    key: encoded.key === null ? null : Buffer.from(encoded.key),
    value: Buffer.from(encoded.value),
    headers: Object.fromEntries(
      Object.entries(encoded.headers).map(([name, value]) => [name, Buffer.from(value)]),
    ),
    offset: '7',
  };
}
