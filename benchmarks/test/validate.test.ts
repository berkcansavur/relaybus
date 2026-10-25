import { describe, expect, it } from 'vitest';
import { handlerNumberOf, validateRun, type OpenDelivery } from '../lib/validate.ts';

const events = [
  { w: 0, n: 0, key: 'k', sequence: 1 },
  { w: 0, n: 1, key: 'k', sequence: 2 },
  { w: 0, n: 2, key: 'k', sequence: 3 },
  { w: 1, n: 0, key: null, sequence: null },
];
const code = (n: number, handler = 1): number => n * 16 + handler;
const all: Array<readonly [number, number, number]> = [
  [0, code(0), 1],
  [0, code(1), 2],
  [0, code(2), 3],
  [1, code(0), 4],
];
const pending: OpenDelivery = { w: 1, n: 0, handler: 'h1', status: 'pending' };

function run(
  handled: ReadonlyArray<readonly [number, number, number]>,
  open: OpenDelivery[] = [],
  leaseLost = 0,
) {
  return validateRun({ handlers: 1, handled, data: { events, open }, drained: true, leaseLost });
}

describe('validateRun', () => {
  it('accepts a complete, ordered run', () => {
    expect(run(all).valid).toBe(true);
  });

  it('detects a lost delivery', () => {
    const result = run(all.slice(0, 3));
    expect(result.lost).toBe(1);
    expect(result.valid).toBe(false);
  });

  it('does not call an open delivery lost, but a drained run may not leave one open', () => {
    const result = run(all.slice(0, 3), [pending]);
    expect(result.lost).toBe(0);
    expect(result.valid).toBe(false);
  });

  it('allows a delivery to remain open when the run did not drain', () => {
    const result = validateRun({
      handlers: 1,
      handled: all.slice(0, 3),
      data: { events, open: [pending] },
      drained: false,
      leaseLost: 0,
    });
    expect(result.valid).toBe(true);
  });

  it('rejects a duplicate unless a lease was lost', () => {
    const duplicated = [...all, [1, code(0), 5] as const];
    expect(run(duplicated).duplicates).toBe(1);
    expect(run(duplicated).valid).toBe(false);
    expect(run(duplicated, [], 1).valid).toBe(true);
  });

  it('detects per-key order violations and gaps', () => {
    expect(
      run([
        [0, code(1), 1],
        [0, code(0), 2],
        [0, code(2), 3],
        [1, code(0), 4],
      ]).orderViolations,
    ).toBeGreaterThan(0);
    expect(
      run([
        [0, code(0), 1],
        [0, code(2), 2],
        [1, code(0), 4],
      ]).orderViolations,
    ).toBeGreaterThan(0);
  });

  it('detects deliveries that were never published and dead letters', () => {
    expect(run([...all, [9, code(0), 6]]).unexpected).toBe(1);
    expect(run(all, [{ ...pending, status: 'dead' }]).valid).toBe(false);
  });
});

describe('routed runs', () => {
  const route = (n: number): OpenDelivery => ({
    w: 1,
    n,
    handler: 'relaybus.transport:bench-fake:bench',
    status: 'pending',
  });

  it('reads the handler number of a local handler or of a route', () => {
    expect(handlerNumberOf('h3', false)).toBe(3);
    expect(handlerNumberOf('relaybus.transport:bench-fake:bench', true)).toBe(1);
    expect(handlerNumberOf('relaybus.transport:bench-fake:bench:bench.order.placed.v1', true)).toBe(
      1,
    );
  });

  it('matches an open route delivery with the send that reached the consumer', () => {
    const input = { handlers: 1, handled: all.slice(0, 3), drained: false, leaseLost: 0 };
    const data = { events, open: [route(0)] };
    expect(validateRun({ ...input, data, routed: true }).lost).toBe(0);
    expect(validateRun({ ...input, data }).lost).toBe(1);
  });

  it('still detects a key sent out of order', () => {
    const swapped: Array<readonly [number, number, number]> = [
      [0, code(1), 1],
      [0, code(0), 2],
      [0, code(2), 3],
      [1, code(0), 4],
    ];
    const result = validateRun({
      handlers: 1,
      handled: swapped,
      data: { events, open: [] },
      drained: true,
      leaseLost: 0,
      routed: true,
    });
    expect(result.orderViolations).toBeGreaterThan(0);
    expect(result.valid).toBe(false);
  });
});
