// Exactness checks for a drain or mixed run. A run whose validation fails is INVALID: its numbers must not be quoted.
//
// Every handler invocation is reported by the worker as [w, n * 16 + handlerNumber, timestamp], where (w, n) identifies
// the published event. The database is the ground truth: `events` holds every committed event, `open` every delivery that
// is not completed (pending, processing or dead). Per run the checks are:
//   lost         expected (event, handler) pairs that were neither handled nor are still open in the database
//   unexpected   handled pairs that were never published
//   duplicates   handler invocations beyond the first per pair; allowed only as at-least-once redeliveries, so they must
//                not exceed the number of lease-lost reports of the workers
//   order        per (handler, key) the handled sequence must follow publish (commit) order and be a prefix of it
//   drained      when the run finished, nothing may be left pending, processing or dead
export interface EventFact {
  w: number;
  n: number;
  key: string | null;
  sequence: number | null;
}

export interface OpenDelivery {
  w: number;
  n: number;
  handler: string;
  status: string;
}

export interface ValidationData {
  events: EventFact[];
  open: OpenDelivery[];
}

export interface Validation {
  valid: boolean;
  reasons: string[];
  expectedPairs: number;
  handledInvocations: number;
  handledDistinct: number;
  duplicates: number;
  leaseLost: number;
  lost: number;
  unexpected: number;
  handledButStillOpen: number;
  openAtEnd: number;
  deadAtEnd: number;
  orderViolations: number;
  keyedEvents: number;
}

/**
 * The handler number of an open delivery. Local handlers are named `h<number>`; in a routed run the single handler is the
 * route to the fake transport, whose id differs between releases (`relaybus.transport:<transport>:<destination>[:<type>]`).
 */
export function handlerNumberOf(handler: string, routed: boolean): number {
  return routed ? 1 : Number(handler.slice(1));
}

export function validateRun(input: {
  handlers: number;
  handled: ReadonlyArray<readonly [number, number, number]>;
  data: ValidationData;
  /** The run handled everything it was meant to; then nothing may remain open. */
  drained: boolean;
  leaseLost: number;
  /** The only handler is a route to a transport (see `handlerNumberOf`). */
  routed?: boolean;
}): Validation {
  const { handlers, handled, data } = input;
  const id = (w: number, n: number): string => `${w}:${n}`;
  const expectedEvents = new Map(data.events.map((event) => [id(event.w, event.n), event]));
  const expected = new Set<string>();
  for (const event of data.events)
    for (let h = 1; h <= handlers; h += 1) expected.add(`${id(event.w, event.n)}:${h}`);

  const handledSet = new Set<string>();
  let unexpected = 0;
  const perKey = new Map<string, Array<[number, number]>>();
  for (const [w, code, ts] of handled) {
    const n = Math.floor(code / 16);
    const handlerNumber = code % 16;
    const pair = `${id(w, n)}:${handlerNumber}`;
    handledSet.add(pair);
    if (!expected.has(pair)) {
      unexpected += 1;
      continue;
    }
    const event = expectedEvents.get(id(w, n))!;
    if (event.key !== null && event.sequence !== null) {
      const group = `${handlerNumber}\u0000${event.key}`;
      if (!perKey.has(group)) perKey.set(group, []);
      perKey.get(group)!.push([ts, event.sequence]);
    }
  }
  const openSet = new Set(
    data.open.map(
      (delivery) =>
        `${id(delivery.w, delivery.n)}:${handlerNumberOf(delivery.handler, input.routed === true)}`,
    ),
  );
  let lost = 0;
  for (const pair of expected) if (!handledSet.has(pair) && !openSet.has(pair)) lost += 1;
  let handledButStillOpen = 0;
  for (const pair of openSet) if (handledSet.has(pair)) handledButStillOpen += 1;

  // Order: collapse immediate repeats (a redelivery of the same head), then the sequence must increase and have no gap.
  let orderViolations = 0;
  for (const entries of perKey.values()) {
    entries.sort((a, b) => a[0] - b[0]);
    const sequences: number[] = [];
    for (const [, sequence] of entries)
      if (sequences[sequences.length - 1] !== sequence) sequences.push(sequence);
    for (let i = 1; i < sequences.length; i += 1)
      if (sequences[i]! <= sequences[i - 1]!) orderViolations += 1;
    const distinct = [...new Set(sequences)].sort((a, b) => a - b);
    // A key's events are numbered 1..m in commit order; the handled ones must be a prefix of that numbering.
    if (distinct.some((sequence, index) => sequence !== index + 1)) orderViolations += 1;
  }
  const deadAtEnd = data.open.filter((delivery) => delivery.status === 'dead').length;
  const duplicates = handled.length - handledSet.size + unexpected;
  const reasons: string[] = [];
  if (lost > 0) reasons.push(`${lost} deliveries lost (neither handled nor open in the database)`);
  if (unexpected > 0) reasons.push(`${unexpected} handled deliveries were never published`);
  if (duplicates - unexpected > input.leaseLost)
    reasons.push(
      `${duplicates - unexpected} duplicate invocations but only ${input.leaseLost} lease-lost reports`,
    );
  if (orderViolations > 0) reasons.push(`${orderViolations} per-key order violations`);
  if (deadAtEnd > 0) reasons.push(`${deadAtEnd} dead letters`);
  if (input.drained && data.open.length > 0)
    reasons.push(`${data.open.length} deliveries still open after the run drained`);
  return {
    valid: reasons.length === 0,
    reasons,
    expectedPairs: expected.size,
    handledInvocations: handled.length,
    handledDistinct: handledSet.size - unexpected,
    duplicates: duplicates - unexpected,
    leaseLost: input.leaseLost,
    lost,
    unexpected,
    handledButStillOpen,
    openAtEnd: data.open.length,
    deadAtEnd,
    orderViolations,
    keyedEvents: data.events.filter((event) => event.key !== null).length,
  };
}
