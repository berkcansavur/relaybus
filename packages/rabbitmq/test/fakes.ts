import { EventEmitter } from 'node:events';
import { vi } from 'vitest';
import type { EventEnvelope, RelayBusLogger } from '@relaybus/core';
import type {
  AmqpChannel,
  AmqpConfirmChannel,
  AmqpConnection,
  AmqpMessage,
  AmqpPublishOptions,
} from '../src/amqp-types.js';
import { encodeEnvelope } from '../src/wire-format.js';

export function createEnvelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: '0190a000-0000-7000-8000-000000000001',
    type: 'order.placed.v1',
    source: 'orders-service',
    key: 'order-1',
    payload: { orderId: '1' },
    headers: { traceparent: '00-abc-def-01' },
    correlationId: 'flow-1',
    causationId: 'cause-1',
    occurredAt: new Date('2026-01-01T10:20:30.456Z'),
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

/** Builds the message amqplib would deliver for an envelope. */
export function createDelivery(envelope: EventEnvelope = createEnvelope()): AmqpMessage {
  const { content, options } = encodeEnvelope(envelope);
  return {
    content,
    fields: { routingKey: 'order.placed.v1' },
    properties: {
      messageId: options.messageId,
      type: options.type,
      appId: options.appId,
      timestamp: options.timestamp,
      headers: options.headers,
    },
  };
}

type ConfirmCallback = (error: Error | null | undefined) => void;

/** Channel fake: every operation succeeds unless a test overrides the mock. */
export class FakeChannel extends EventEmitter implements AmqpConfirmChannel {
  deliver: ((message: AmqpMessage | null) => void) | undefined;
  readonly published: { routingKey: string; content: Buffer; options: AmqpPublishOptions }[] = [];

  assertExchange = vi.fn(
    async (_exchange: string, _type: string, _options?: unknown): Promise<unknown> => ({}),
  );
  assertQueue = vi.fn(async (_queue: string, _options?: unknown): Promise<unknown> => ({}));
  bindQueue = vi.fn(
    async (_queue: string, _source: string, _pattern: string): Promise<unknown> => ({}),
  );
  prefetch = vi.fn(async (_count: number): Promise<unknown> => ({}));
  cancel = vi.fn(async (_consumerTag: string): Promise<unknown> => ({}));
  ack = vi.fn<(message: AmqpMessage) => void>();
  nack = vi.fn<(message: AmqpMessage, allUpTo?: boolean, requeue?: boolean) => void>();
  close = vi.fn(async (): Promise<void> => {
    this.emit('close');
  });
  consume = vi.fn(
    async (
      _queue: string,
      onMessage: (message: AmqpMessage | null) => void,
    ): Promise<{ consumerTag: string }> => {
      this.deliver = onMessage;
      return { consumerTag: 'consumer-1' };
    },
  );
  /** Default behaviour: confirm immediately. Tests replace it to nack, hang or throw. */
  publishBehaviour: (callback: ConfirmCallback, routingKey: string) => void = (callback) => {
    callback(null);
  };

  publish(
    _exchange: string,
    routingKey: string,
    content: Buffer,
    options: AmqpPublishOptions,
    callback: ConfirmCallback,
  ): boolean {
    this.published.push({ routingKey, content, options });
    this.publishBehaviour(callback, routingKey);
    return true;
  }
}

export class FakeConnection extends EventEmitter implements AmqpConnection {
  readonly channels: FakeChannel[] = [];
  readonly confirmChannels: FakeChannel[] = [];
  createChannel = vi.fn(async (): Promise<AmqpChannel> => {
    const channel = new FakeChannel();
    this.channels.push(channel);
    return channel;
  });
  createConfirmChannel = vi.fn(async (): Promise<AmqpConfirmChannel> => {
    const channel = new FakeChannel();
    this.confirmChannels.push(channel);
    return channel;
  });
  close = vi.fn(async (): Promise<void> => {
    this.emit('close');
  });

  get publisher(): FakeChannel {
    return this.confirmChannels.at(-1)!;
  }

  get consumerChannel(): FakeChannel {
    return this.channels.at(-1)!;
  }
}

export async function waitFor(
  condition: () => boolean,
  timeoutMs = 5_000,
  description = 'condition',
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

export function deferred<TValue = void>(): {
  promise: Promise<TValue>;
  resolve: (value: TValue) => void;
  reject: (error: unknown) => void;
} {
  let resolve!: (value: TValue) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<TValue>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}
