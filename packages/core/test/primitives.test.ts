import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import {
  ConfigurationError,
  DEFAULT_RETRY_POLICY,
  EventValidationError,
  NonRetryableError,
  RelayBusError,
  computeRetryDelay,
  createRetryPolicy,
  createUuidV7,
  defineEvent,
  isRetryableError,
  systemClock,
  toErrorInfo,
  toMilliseconds,
  validatePayload,
  type StandardSchemaV1,
} from '@relaybus/core';
import { getCurrentScope, runInIsolatedScope, runInScope } from '../src/execution-scope.js';

function schemaOf<T>(
  validate: (value: unknown) => StandardSchemaV1.Result<T> | Promise<StandardSchemaV1.Result<T>>,
): StandardSchemaV1<unknown, T> {
  return { '~standard': { version: 1, vendor: 'test', validate } };
}

describe('toMilliseconds', () => {
  it.each([
    [250, 250],
    ['250ms', 250],
    ['1.5s', 1_500],
    ['2m', 120_000],
    ['1h', 3_600_000],
    ['7d', 604_800_000],
    [0, 0],
  ] as const)('converts %s', (input, expected) => {
    expect(toMilliseconds(input, 'value')).toBe(expected);
  });

  it.each([-1, Number.NaN, Number.POSITIVE_INFINITY])('rejects the number %s', (input) => {
    expect(() => toMilliseconds(input, 'value')).toThrow(ConfigurationError);
  });

  it.each(['10', '5 s', 's', '-1s', '1w'])('rejects the string %s', (input) => {
    expect(() => toMilliseconds(input as never, 'value')).toThrow(/value must look like/);
  });
});

describe('errors', () => {
  it('names errors after their class', () => {
    expect(new NonRetryableError('x').name).toBe('NonRetryableError');
    expect(new RelayBusError('x', { cause: 'y' }).cause).toBe('y');
  });

  it('snapshots errors and non-errors', () => {
    const error = new TypeError('bad');
    expect(toErrorInfo(error)).toEqual({ name: 'TypeError', message: 'bad', stack: error.stack });
    expect(toErrorInfo('text')).toEqual({ name: 'NonError', message: 'text' });
    expect(toErrorInfo({ code: 1 })).toEqual({ name: 'NonError', message: '{"code":1}' });
    expect(toErrorInfo(undefined)).toEqual({ name: 'NonError', message: 'undefined' });
    const circular: Record<string, unknown> = {};
    circular['self'] = circular;
    expect(toErrorInfo(circular)).toEqual({ name: 'NonError', message: '[object Object]' });
  });
});

describe('createUuidV7', () => {
  it('produces RFC 9562 version 7 identifiers ordered by time', () => {
    const first = createUuidV7(1_700_000_000_000);
    const second = createUuidV7(1_700_000_000_001);
    expect(first).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(first.slice(0, 13)).toBe('018bcfe5-6800');
    expect(first < second).toBe(true);
    expect(createUuidV7()).not.toBe(createUuidV7());
  });
});

describe('systemClock', () => {
  it('returns the current time', () => {
    const before = Date.now();
    const now = systemClock.now().getTime();
    expect(now).toBeGreaterThanOrEqual(before);
  });
});

describe('defineEvent', () => {
  it('creates a named event class carrying its type', () => {
    class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}
    const event = new OrderPlaced({ orderId: 'o-1' });
    expect(OrderPlaced.type).toBe('order.placed.v1');
    expect(OrderPlaced.schema).toBeUndefined();
    expect(event).toMatchObject({ type: 'order.placed.v1', payload: { orderId: 'o-1' } });
  });

  it('attaches a schema', () => {
    const schema = schemaOf<{ id: number }>((value) => ({ value: value as { id: number } }));
    class Typed extends defineEvent('typed.v1', { schema }) {}
    expect(Typed.schema).toBe(schema);
  });

  it.each(['', '.starts-with-dot', 'has space', 'x'.repeat(256)])('rejects the type %j', (type) => {
    expect(() => defineEvent(type)).toThrow(ConfigurationError);
  });
});

describe('validatePayload', () => {
  it('passes payloads through without a schema', async () => {
    const payload = { a: 1 };
    await expect(validatePayload('t', undefined, payload)).resolves.toBe(payload);
  });

  it('returns the schema output, synchronous or not', async () => {
    const sync = schemaOf((value) => ({ value: { parsed: value } }));
    const async = schemaOf(async (value) => ({ value: { parsed: value } }));
    await expect(validatePayload('t', sync, 1)).resolves.toEqual({ parsed: 1 });
    await expect(validatePayload('t', async, 2)).resolves.toEqual({ parsed: 2 });
  });

  it('throws a non-retryable validation error with normalized paths', async () => {
    const schema = schemaOf(() => ({
      issues: [
        { message: 'required', path: ['order', { key: 'id' }, 0] },
        { message: 'too small' },
      ],
    }));
    const error = await validatePayload('order.placed.v1', schema, {}).catch(
      (caught: unknown) => caught,
    );
    expect(error).toBeInstanceOf(EventValidationError);
    expect(error).toBeInstanceOf(NonRetryableError);
    expect((error as EventValidationError).message).toBe(
      "Payload of 'order.placed.v1' failed validation: required; too small",
    );
    expect((error as EventValidationError).issues).toEqual([
      { message: 'required', path: ['order', 'id', 0] },
      { message: 'too small', path: undefined },
    ]);
  });
});

describe('retry policy', () => {
  it('has documented defaults', () => {
    expect(createRetryPolicy(undefined)).toEqual(DEFAULT_RETRY_POLICY);
    expect(DEFAULT_RETRY_POLICY).toMatchObject({
      attempts: 10,
      initialDelayMs: 1_000,
      factor: 2,
      maxDelayMs: 600_000,
      jitter: 'full',
    });
  });

  it('overrides fields on top of a base policy', () => {
    const retryIf = () => true;
    const base = createRetryPolicy({ attempts: 3, initialDelay: '2s' });
    expect(
      createRetryPolicy({ maxDelay: '1m', factor: 3, jitter: 'equal', retryIf }, base),
    ).toEqual({
      attempts: 3,
      initialDelayMs: 2_000,
      factor: 3,
      maxDelayMs: 60_000,
      jitter: 'equal',
      retryIf,
    });
  });

  it.each([
    [{ attempts: 0 }, /attempts must be a positive integer/],
    [{ attempts: 1.5 }, /attempts must be a positive integer/],
    [{ factor: 0.5 }, /factor must be at least 1/],
    [{ factor: Number.NaN }, /factor must be at least 1/],
    [{ initialDelay: '2m', maxDelay: '1m' }, /maxDelay must not be smaller/],
    [{ jitter: 'random' }, /jitter must be one of full, equal, none/],
  ])('rejects %j', (options, message) => {
    expect(() => createRetryPolicy(options as never)).toThrow(message);
  });

  it('grows exponentially up to the cap', () => {
    const policy = createRetryPolicy({
      jitter: 'none',
      initialDelay: 100,
      factor: 2,
      maxDelay: 500,
    });
    expect([1, 2, 3, 4, 5].map((attempt) => computeRetryDelay(policy, attempt))).toEqual([
      100, 200, 400, 500, 500,
    ]);
  });

  it('applies full and equal jitter', () => {
    const full = createRetryPolicy({ jitter: 'full', initialDelay: 1_000 });
    const equal = createRetryPolicy({ jitter: 'equal', initialDelay: 1_000 });
    expect(computeRetryDelay(full, 1, () => 0.25)).toBe(250);
    expect(computeRetryDelay(equal, 1, () => 0.25)).toBe(625);
    expect(computeRetryDelay(full, 1)).toBeLessThanOrEqual(1_000);
  });

  it('keeps every delay within [0, maxDelay]', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 100 }),
        fc.integer({ min: 1, max: 1_000_000 }),
        fc.double({ min: 1, max: 10, noNaN: true }),
        fc.constantFrom('full', 'equal', 'none'),
        fc.double({ min: 0, max: 1, maxExcluded: true, noNaN: true }),
        (attempt, initialDelay, factor, jitter, random) => {
          const policy = createRetryPolicy({
            initialDelay,
            maxDelay: initialDelay * 10,
            factor,
            jitter: jitter as 'full',
          });
          const delay = computeRetryDelay(policy, attempt, () => random);
          return delay >= 0 && delay <= policy.maxDelayMs;
        },
      ),
    );
  });

  it('classifies errors', () => {
    const always = createRetryPolicy(undefined);
    expect(isRetryableError(always, new Error('x'))).toBe(true);
    expect(isRetryableError(always, new NonRetryableError('x'))).toBe(false);
    const onlyTypeErrors = createRetryPolicy({ retryIf: (error) => error instanceof TypeError });
    expect(isRetryableError(onlyTypeErrors, new TypeError('x'))).toBe(true);
    expect(isRetryableError(onlyTypeErrors, new Error('x'))).toBe(false);
    const broken = createRetryPolicy({
      retryIf: () => {
        throw new Error('bug');
      },
    });
    expect(isRetryableError(broken, new Error('x'))).toBe(true);
  });
});

describe('execution scope', () => {
  it('nests scopes and restores the outer one', () => {
    expect(getCurrentScope()).toBeUndefined();
    runInScope({ transaction: 'outer' }, () => {
      runInScope({ event: { id: 'e' } as never }, () => {
        expect(getCurrentScope()).toEqual({ transaction: 'outer', event: { id: 'e' } });
      });
      expect(getCurrentScope()).toEqual({ transaction: 'outer' });
      runInIsolatedScope({ event: { id: 'e' } as never }, () => {
        expect(getCurrentScope()).toEqual({ event: { id: 'e' } });
      });
    });
  });
});
