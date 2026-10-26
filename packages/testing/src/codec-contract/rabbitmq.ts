import type { EventEnvelope } from '@relaybus/core';
import { describe, expect, it } from 'vitest';
import {
  attempt,
  compareBodies,
  type Attempt,
  compareDecoded,
  compareObjects,
  toBuffer,
  type BodyComparison,
  type WireBytes,
} from './compare.js';

/** A message as captured from a queue: what an existing publisher wrote. */
export interface RabbitMqCapturedMessage {
  readonly routingKey: string;
  /**
   * The message properties as publish options name them (`persistent`, `messageId`,
   * `contentType`, `timestamp`, ...), without `headers`. Default none.
   */
  readonly properties?: Readonly<Record<string, unknown>>;
  /** Default none. */
  readonly headers?: Readonly<Record<string, unknown>>;
  readonly body: WireBytes;
}

/** What a codec's `encode` returns: a `RabbitMqCodec` message. */
export interface RabbitMqWrittenMessage {
  readonly content: Uint8Array;
  /** The publish options: properties, and the headers under `headers`. */
  readonly options: object;
}

/** A message as a consumer receives it. */
export interface RabbitMqDeliveredMessage {
  readonly content: Buffer;
  readonly fields: { readonly routingKey: string };
  readonly properties: Readonly<Record<string, unknown>> & {
    readonly headers: Readonly<Record<string, unknown>>;
  };
}

/** One message of the format: the event and the message that carries it. */
export interface RabbitMqCodecCase {
  readonly name: string;
  readonly envelope: EventEnvelope;
  readonly message: RabbitMqCapturedMessage;
  /** The event decoding the message gives, when the wire does not carry all of `envelope`. */
  readonly decoded?: EventEnvelope;
  /**
   * Checks one direction only: `'decode'` for a message only other producers write (for
   * example one without an id, which relaybus would add), `'encode'` for one only other
   * consumers read. Default both.
   */
  readonly only?: 'encode' | 'decode';
}

export interface RabbitMqCodecContractOptions {
  /** Writes an event: a codec's `encode`. The routing key is the route's destination, not the codec's. */
  readonly encode: (
    envelope: EventEnvelope,
  ) => RabbitMqWrittenMessage | Promise<RabbitMqWrittenMessage>;
  /** Reads a message the way the transport does: `(message) => decodeRabbitMqMessage(codec, message)`. */
  readonly decode: (message: RabbitMqDeliveredMessage) => EventEnvelope | Promise<EventEnvelope>;
  readonly cases: readonly RabbitMqCodecCase[];
  /** How bodies are compared. Default `'bytes'`. */
  readonly compare?: BodyComparison;
  /** Properties left out of the comparison, such as a `timestamp` that differs per send. */
  readonly ignoreProperties?: readonly string[];
  /** Headers left out of the comparison. */
  readonly ignoreHeaders?: readonly string[];
}

/**
 * What differs between `testCase` and what the codec writes and reads, one line each: the
 * properties, headers and body of `encode(envelope)`, and `decode(message)` against the expected
 * event. Empty when they agree.
 */
export async function rabbitMqCodecMismatches(
  options: RabbitMqCodecContractOptions,
  testCase: RabbitMqCodecCase,
): Promise<string[]> {
  const { message } = testCase;
  const encoding: Attempt<RabbitMqWrittenMessage> =
    testCase.only === 'decode'
      ? { mismatches: [] }
      : await attempt('encode', () => options.encode(testCase.envelope));
  const encoded = encoding.result;
  const decoding: Attempt<EventEnvelope> =
    testCase.only === 'encode'
      ? { mismatches: [] }
      : await attempt('decode', () => options.decode(delivered(message)));
  return [
    ...encoding.mismatches,
    ...(encoded === undefined ? [] : compareMessages(options, { message, encoded })),
    ...decoding.mismatches,
    ...(decoding.result === undefined
      ? []
      : compareDecoded(testCase.decoded ?? testCase.envelope, decoding.result)),
  ];
}

/** Registers one test per case asserting {@link rabbitMqCodecMismatches} finds nothing. */
export function rabbitMqCodecContract(name: string, options: RabbitMqCodecContractOptions): void {
  describe(`${name} writes and reads the captured RabbitMQ messages`, () => {
    it.each(options.cases.map((testCase) => [testCase.name, testCase] as const))(
      '%s',
      async (_name, testCase) => {
        expect(await rabbitMqCodecMismatches(options, testCase)).toEqual([]);
      },
    );
  });
}

function compareMessages(
  options: RabbitMqCodecContractOptions,
  {
    message,
    encoded,
  }: { readonly message: RabbitMqCapturedMessage; readonly encoded: RabbitMqWrittenMessage },
): string[] {
  const { headers, ...properties } = encoded.options as {
    readonly headers?: Readonly<Record<string, unknown>>;
  };
  return [
    ...compareObjects('properties', {
      expected: message.properties ?? {},
      encoded: properties,
      ignored: options.ignoreProperties ?? [],
    }),
    ...compareObjects('headers', {
      expected: message.headers ?? {},
      encoded: headers ?? {},
      ignored: options.ignoreHeaders ?? [],
    }),
    ...compareBodies('body', {
      expected: message.body,
      encoded: encoded.content,
      comparison: options.compare ?? 'bytes',
    }),
  ];
}

function delivered(message: RabbitMqCapturedMessage): RabbitMqDeliveredMessage {
  return {
    content: toBuffer(message.body),
    fields: { routingKey: message.routingKey },
    properties: { ...message.properties, headers: { ...message.headers } },
  };
}
