import { afterEach, describe, expect, it, vi } from 'vitest';
import type { EventEnvelope } from '@relaybus/core';
import {
  CodecContractError,
  assertKafkaCodecCase,
  assertRabbitMqCodecCase,
  registerKafkaCodecContract,
  registerRabbitMqCodecContract,
  type KafkaCodecCase,
  type KafkaCodecContractOptions,
  type RabbitMqCodecCase,
  type RabbitMqCodecContractOptions,
  type TestFramework,
} from '../src/codec-contract/index.js';
import * as jest from '../src/jest.js';

const ENVELOPE: EventEnvelope = {
  id: 'evt-1',
  type: 'order.placed.v1',
  source: 'orders',
  key: null,
  payload: { zz: 1, a: { y: 2, b: 1 } },
  headers: {},
  correlationId: null,
  causationId: null,
  occurredAt: new Date('2026-01-01T00:00:00.000Z'),
};

const KAFKA_CASE: KafkaCodecCase = {
  name: 'order placed',
  envelope: ENVELOPE,
  record: { topic: 'orders', key: null, value: '{"zz":1,"a":{"y":2,"b":1}}' },
};

/** A codec that writes the payload as the value; `parse` reads it back. */
function kafkaOptions(parse: (text: string) => unknown = JSON.parse): KafkaCodecContractOptions {
  return {
    encode: (envelope) => ({ key: null, value: JSON.stringify(envelope.payload), headers: {} }),
    decode: (record) => ({ ...ENVELOPE, payload: parse(record.value!.toString()) }),
    cases: [KAFKA_CASE],
  };
}

/** Parses JSON and sorts the keys at the top level, as a re-ordering store or codec would. */
function sortingParse(text: string): unknown {
  const value = JSON.parse(text) as Record<string, unknown>;
  return Object.fromEntries(
    Object.entries(value).sort(([left], [right]) => left.localeCompare(right)),
  );
}

const RABBIT_CASE: RabbitMqCodecCase = {
  name: 'order placed',
  envelope: ENVELOPE,
  message: { routingKey: 'orders', body: '{"zz":1,"a":{"y":2,"b":1}}' },
};

function rabbitOptions(
  parse: (text: string) => unknown = JSON.parse,
): RabbitMqCodecContractOptions {
  return {
    encode: (envelope) => ({ content: Buffer.from(JSON.stringify(envelope.payload)), options: {} }),
    decode: (message) => ({ ...ENVELOPE, payload: parse(message.content.toString()) }),
    cases: [RABBIT_CASE],
  };
}

/** A test framework that records what is registered and runs the tests on request. */
function recordingFramework() {
  const titles: string[] = [];
  const tests: [string, () => Promise<void>][] = [];
  const framework: TestFramework = {
    describe: (name, body) => {
      titles.push(name);
      body();
    },
    it: (name, body) => {
      tests.push([name, body]);
    },
  };
  return { framework, titles, tests };
}

describe('codec contract assertions without a test framework', () => {
  it('pass when the codec writes and reads the case exactly', async () => {
    await expect(assertKafkaCodecCase(kafkaOptions(), KAFKA_CASE)).resolves.toBeUndefined();
    await expect(assertRabbitMqCodecCase(rabbitOptions(), RABBIT_CASE)).resolves.toBeUndefined();
  });

  it('throw a plain error listing the mismatches, a re-ordered payload included', async () => {
    const failure = await assertKafkaCodecCase(kafkaOptions(sortingParse), KAFKA_CASE).catch(
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(CodecContractError);
    expect(failure).toBeInstanceOf(Error);
    expect((failure as CodecContractError).name).toBe('CodecContractError');
    expect((failure as CodecContractError).mismatches).toEqual([
      'decoded payload: expected {"zz":1,"a":{"y":2,"b":1}}, decoded {"a":{"y":2,"b":1},"zz":1} (same values, other key order)',
    ]);
    expect((failure as CodecContractError).message).toBe(
      'Codec case \'order placed\' does not match:\n- decoded payload: expected {"zz":1,"a":{"y":2,"b":1}}, decoded {"a":{"y":2,"b":1},"zz":1} (same values, other key order)',
    );
    await expect(
      assertRabbitMqCodecCase(rabbitOptions(sortingParse), RABBIT_CASE),
    ).rejects.toBeInstanceOf(CodecContractError);
    await expect(
      assertRabbitMqCodecCase({ ...rabbitOptions(sortingParse), compare: 'json' }, RABBIT_CASE),
    ).resolves.toBeUndefined();
  });
});

describe('registering the codec contract with any test framework', () => {
  it('registers one test per case under one describe, each asserting its case', async () => {
    const kafka = recordingFramework();
    registerKafkaCodecContract(kafka.framework, { name: 'orders', options: kafkaOptions() });
    expect(kafka.titles).toEqual(['orders writes and reads the captured Kafka records']);
    expect(kafka.tests.map(([name]) => name)).toEqual(['order placed']);
    await expect(kafka.tests[0]![1]()).resolves.toBeUndefined();

    const rabbit = recordingFramework();
    registerRabbitMqCodecContract(rabbit.framework, {
      name: 'orders',
      options: rabbitOptions(sortingParse),
    });
    expect(rabbit.titles).toEqual(['orders writes and reads the captured RabbitMQ messages']);
    await expect(rabbit.tests[0]![1]()).rejects.toBeInstanceOf(CodecContractError);
  });
});

describe('@relaybus/testing/jest', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('registers with the describe and it globals', async () => {
    const recorded = recordingFramework();
    vi.stubGlobal('describe', recorded.framework.describe);
    vi.stubGlobal('it', recorded.framework.it);
    jest.kafkaCodecContract('orders', kafkaOptions());
    jest.rabbitMqCodecContract('orders', rabbitOptions());
    expect(recorded.titles).toEqual([
      'orders writes and reads the captured Kafka records',
      'orders writes and reads the captured RabbitMQ messages',
    ]);
    await Promise.all(recorded.tests.map(([, test]) => test()));
    expect(jest.CodecContractError).toBe(CodecContractError);
  });

  it('explains what to use when the runner has no such globals', () => {
    vi.stubGlobal('describe', undefined);
    expect(() => jest.kafkaCodecContract('orders', kafkaOptions())).toThrow(
      /needs the describe and it globals of Jest/,
    );
    vi.stubGlobal('describe', () => undefined);
    vi.stubGlobal('it', undefined);
    expect(() => jest.rabbitMqCodecContract('orders', rabbitOptions())).toThrow(
      /registerKafkaCodecContract or registerRabbitMqCodecContract/,
    );
  });
});
