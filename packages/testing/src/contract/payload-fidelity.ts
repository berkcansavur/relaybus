import { describe, expect, it } from 'vitest';
import { at, createDelivery, createEnvelope } from './contract-fixtures.js';
import type { ContractSuite } from './contract-suite.js';

/**
 * A payload whose serialized form a store that normalizes JSON would change: keys out of
 * alphabetical and length order at every depth, arrays of objects, unicode, numbers JSON writes in
 * exponent form, the largest safe integer, and empty containers.
 */
function unsortedPayload(): Record<string, unknown> {
  return {
    zulu: 'last letter first',
    b: { yankee: [{ zz: 1, a: 2, mm: { q: null, c: true } }, [], {}], alpha: {} },
    longestKeyOfTheObject: 1.0,
    a: 'çğıöşü 🚀   "quoted" \\ tab\t',
    exponent: 1e21,
    tiny: 5e-324,
    fraction: 0.1 + 0.2,
    negative: -42.5,
    maxSafe: Number.MAX_SAFE_INTEGER,
    minSafe: Number.MIN_SAFE_INTEGER,
    emptyObject: {},
    emptyArray: [],
    '7': 'integer-like key',
    mixed: ['z', { d: 4, c: 3 }, 0, false, null],
  };
}

/** The payload and headers a store hands back must serialize byte for byte as they were published. */
export function payloadFidelityContract<TTransaction>(suite: ContractSuite<TTransaction>): void {
  const { append, claim } = suite;

  describe('payload fidelity', () => {
    it('hands out the payload and headers with their key order, as published', async () => {
      const event = createEnvelope({
        payload: unsortedPayload(),
        headers: { zz: 'last', traceparent: '00-abc-def-01', a: 'first', tenant: 'acme' },
      });
      await append([{ event, deliveries: [createDelivery('handler-a')] }]);
      const [claimed] = await claim(['handler-a']);
      expect(JSON.stringify(claimed!.event.payload)).toBe(JSON.stringify(event.payload));
      expect(JSON.stringify(claimed!.event.headers)).toBe(JSON.stringify(event.headers));
    });

    it('keeps the order of a payload appended in a batch and of a top-level array', async () => {
      const object = createEnvelope({ payload: unsortedPayload() });
      const list = createEnvelope({ payload: [{ zz: 1, a: { y: 2, b: 1 } }, 'x', 1e21] });
      const scalar = createEnvelope({ payload: 'plain text' });
      await append([
        { event: object, deliveries: [createDelivery('handler-a', at(1))] },
        { event: list, deliveries: [createDelivery('handler-a', at(2))] },
        { event: scalar, deliveries: [createDelivery('handler-a', at(3))] },
      ]);
      const claimed = await claim(['handler-a'], at(10));
      expect(claimed.map((delivery) => JSON.stringify(delivery.event.payload))).toEqual(
        [object, list, scalar].map((event) => JSON.stringify(event.payload)),
      );
    });

    it('keeps the order for a handler added to an event that already exists', async () => {
      const event = createEnvelope({ payload: unsortedPayload() });
      await append([{ event, deliveries: [createDelivery('handler-a')] }]);
      await append([{ event, deliveries: [createDelivery('handler-b')] }]);
      const [claimed] = await claim(['handler-b']);
      expect(JSON.stringify(claimed!.event.payload)).toBe(JSON.stringify(event.payload));
    });
  });
}
