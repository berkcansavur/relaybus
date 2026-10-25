import type { EventEnvelope } from '@relaybus/core';
import { describe, expect, it } from 'vitest';
import {
  attempt,
  compareBodies,
  compareDecoded,
  compareObjects,
  toBuffer,
  type BodyComparison,
  type WireBytes,
} from './compare.js';

/** A record as captured from a topic: what an existing producer wrote. */
export interface KafkaCapturedRecord {
  readonly topic: string;
  /** Default 0. */
  readonly partition?: number;
  /** Default `'0'`. */
  readonly offset?: string;
  readonly key: WireBytes | null;
  readonly value: WireBytes | null;
  /** Default none. */
  readonly headers?: Readonly<Record<string, WireBytes>>;
  /** Epoch milliseconds in a string; compared only when the codec sets a timestamp. */
  readonly timestamp?: string;
}

/** What a codec's `encode` returns: a `KafkaCodec` record. */
export interface KafkaWrittenRecord {
  readonly key: WireBytes | null;
  readonly value: WireBytes | null;
  readonly headers: Readonly<Record<string, WireBytes | readonly WireBytes[]>>;
  readonly timestamp?: string;
}

/** A record as a consumer hands it over: bytes for the key, the value and every header. */
export interface KafkaDeliveredRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly key: Buffer | null;
  readonly value: Buffer | null;
  readonly headers: Readonly<Record<string, Buffer>>;
  readonly timestamp?: string;
}

/** One message of the format: the event and the record that carries it. */
export interface KafkaCodecCase {
  readonly name: string;
  readonly envelope: EventEnvelope;
  readonly record: KafkaCapturedRecord;
  /**
   * The event decoding the record gives, when the wire does not carry all of `envelope` (for
   * example a record written without a key). Default `envelope`.
   */
  readonly decoded?: EventEnvelope;
}

export interface KafkaCodecContractOptions {
  /** Writes an event: a codec's `encode`, or a transport's publish captured by an injected producer. */
  readonly encode: (envelope: EventEnvelope) => KafkaWrittenRecord | Promise<KafkaWrittenRecord>;
  /** Reads a record the way the transport does: `(record) => decodeKafkaRecord(codec, record)`. */
  readonly decode: (record: KafkaDeliveredRecord) => EventEnvelope | Promise<EventEnvelope>;
  readonly cases: readonly KafkaCodecCase[];
  /** How values are compared. Default `'bytes'`. */
  readonly compare?: BodyComparison;
  /** Headers left out of the comparison, such as ones relaybus adds and other consumers ignore. */
  readonly ignoreHeaders?: readonly string[];
}

/**
 * What differs between `testCase` and what the codec writes and reads, one line each: the key,
 * the value, the headers (a repeated header by its last value) and the timestamp of
 * `encode(envelope)`, and `decode(record)` against the expected event. Empty when they agree.
 */
export async function kafkaCodecMismatches(
  options: KafkaCodecContractOptions,
  testCase: KafkaCodecCase,
): Promise<string[]> {
  const { record } = testCase;
  const encoding = await attempt('encode', () => options.encode(testCase.envelope));
  const encoded = encoding.result;
  const decoding = await attempt('decode', () => options.decode(delivered(record)));
  return [
    ...encoding.mismatches,
    ...(encoded === undefined ? [] : compareRecords(options, { record, encoded })),
    ...decoding.mismatches,
    ...(decoding.result === undefined
      ? []
      : compareDecoded(testCase.decoded ?? testCase.envelope, decoding.result)),
  ];
}

/**
 * Registers one test per case asserting {@link kafkaCodecMismatches} finds nothing: a format of
 * existing producers and consumers is adopted only once relaybus writes and reads it exactly.
 */
export function kafkaCodecContract(name: string, options: KafkaCodecContractOptions): void {
  describe(`${name} writes and reads the captured Kafka records`, () => {
    it.each(options.cases.map((testCase) => [testCase.name, testCase] as const))(
      '%s',
      async (_name, testCase) => {
        expect(await kafkaCodecMismatches(options, testCase)).toEqual([]);
      },
    );
  });
}

function compareRecords(
  options: KafkaCodecContractOptions,
  {
    record,
    encoded,
  }: { readonly record: KafkaCapturedRecord; readonly encoded: KafkaWrittenRecord },
): string[] {
  const text = (bytes: WireBytes | null): string | null =>
    bytes === null ? null : toBuffer(bytes).toString('utf8');
  const timestamp = encoded.timestamp;
  return [
    ...(text(record.key) === text(encoded.key)
      ? []
      : [`key: expected ${text(record.key)}, encoded ${text(encoded.key)}`]),
    ...compareBodies('value', {
      expected: record.value,
      encoded: encoded.value,
      comparison: options.compare ?? 'bytes',
    }),
    ...compareObjects('headers', {
      expected: textHeaders(record.headers ?? {}),
      encoded: textHeaders(encoded.headers),
      ignored: options.ignoreHeaders ?? [],
    }),
    ...(timestamp === undefined || timestamp === record.timestamp
      ? []
      : [`timestamp: expected ${record.timestamp}, encoded ${timestamp}`]),
  ];
}

function textHeaders(
  headers: Readonly<Record<string, WireBytes | readonly WireBytes[]>>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(headers).map(([name, value]) => [
      name,
      toBuffer(Array.isArray(value) ? (value.at(-1) as WireBytes) : (value as WireBytes)).toString(
        'utf8',
      ),
    ]),
  );
}

function delivered(record: KafkaCapturedRecord): KafkaDeliveredRecord {
  return {
    topic: record.topic,
    partition: record.partition ?? 0,
    offset: record.offset ?? '0',
    key: record.key === null ? null : toBuffer(record.key),
    value: record.value === null ? null : toBuffer(record.value),
    headers: Object.fromEntries(
      Object.entries(record.headers ?? {}).map(([name, value]) => [name, toBuffer(value)]),
    ),
    ...(record.timestamp === undefined ? {} : { timestamp: record.timestamp }),
  };
}
