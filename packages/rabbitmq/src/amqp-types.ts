import type { EventEmitter } from 'node:events';

/**
 * The slice of amqplib this package relies on. Declaring it here keeps the
 * public types independent of the amqplib version (peer range `>=0.10`) and
 * lets tests substitute fakes. Real amqplib connections and channels satisfy it.
 */

/** Message properties as delivered by the broker. */
export interface AmqpMessageProperties {
  readonly messageId?: unknown;
  readonly type?: unknown;
  readonly appId?: unknown;
  readonly correlationId?: unknown;
  readonly timestamp?: unknown;
  readonly contentType?: unknown;
  readonly headers?: Readonly<Record<string, unknown>> | undefined;
}

/** A message delivered to a consumer or returned by the broker. */
export interface AmqpMessage {
  readonly content: Buffer;
  readonly fields: { readonly routingKey: string };
  readonly properties: AmqpMessageProperties;
}

/** Properties set on a published message. */
export interface AmqpPublishOptions {
  readonly contentType: string;
  readonly persistent: boolean;
  readonly mandatory?: boolean;
  readonly messageId?: string;
  readonly type?: string;
  readonly appId?: string;
  readonly timestamp?: number;
  readonly correlationId?: string;
  readonly headers: Readonly<Record<string, unknown>>;
}

/** Channel used for consuming. */
export interface AmqpChannel extends EventEmitter {
  assertExchange(
    exchange: string,
    type: string,
    options?: { readonly durable?: boolean },
  ): Promise<unknown>;
  assertQueue(
    queue: string,
    options?: { readonly durable?: boolean; readonly arguments?: Record<string, unknown> },
  ): Promise<unknown>;
  bindQueue(queue: string, source: string, pattern: string): Promise<unknown>;
  prefetch(count: number): Promise<unknown>;
  consume(
    queue: string,
    onMessage: (message: AmqpMessage | null) => void,
    options?: { readonly noAck?: boolean },
  ): Promise<{ readonly consumerTag: string }>;
  cancel(consumerTag: string): Promise<unknown>;
  ack(message: AmqpMessage): void;
  nack(message: AmqpMessage, allUpTo?: boolean, requeue?: boolean): void;
  close(): Promise<void>;
}

/** Channel with publisher confirms. */
export interface AmqpConfirmChannel extends AmqpChannel {
  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: AmqpPublishOptions,
    callback: (error: Error | null | undefined) => void,
  ): boolean;
}

/** A broker connection. Emits `close` and `error`. */
export interface AmqpConnection extends EventEmitter {
  createChannel(): Promise<AmqpChannel>;
  createConfirmChannel(): Promise<AmqpConfirmChannel>;
  close(): Promise<void>;
}
