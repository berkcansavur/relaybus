// The measurement plan: which cells run in the quick gate and in the full run, with fixed measurement windows.
// Pure data; the fingerprint of a baseline includes it, so changing a window or a seed starts a fresh baseline.

export type Store = 'pg' | 'mongo';
export type Mode = 'quick' | 'full';

export interface KeySpec {
  readonly mode: 'none' | 'distinct' | 'hot';
  readonly count: number;
}

/** Fixed delay after which the in-process fake transport of a routed cell acks a send. */
export const ROUTED_ACK_MS = 5;

export const NO_KEYS: KeySpec = { mode: 'none', count: 0 };

export interface Windows {
  readonly warmupMs: number;
  readonly durationMs: number;
}

/** A preloaded database that dispatch cells clone instead of draining a fresh one. */
export interface TemplateDefinition {
  readonly name: string;
  readonly store: Store;
  readonly events: number;
  readonly handlers: number;
  readonly keys: KeySpec;
  /** Events per transaction while loading. Keyed templates use one key per transaction. */
  readonly batch: number;
  readonly procs: number;
  readonly loops: number;
  readonly pool: number;
  /** Deliveries are routes to the fake transport instead of local handlers. */
  readonly routed?: boolean;
}

interface CellBase {
  readonly id: string;
  readonly store: Store;
  readonly title: string;
  /** Number of samples per side when the plan is run (quick mode overrides it with the pair count). */
  readonly reps: number;
}

export interface PublishCell extends CellBase, Windows {
  readonly kind: 'publish';
  readonly procs: number;
  readonly loops: number;
  readonly batch: number;
  readonly handlers: number;
  readonly pool: number;
  readonly keys: KeySpec;
}

export interface DispatchSettings {
  readonly instances: number;
  readonly concurrency: number;
  readonly batchSize: number;
  readonly pool: number;
  readonly pollMs: number;
}

export interface DispatchCell extends CellBase, Windows, DispatchSettings {
  readonly kind: 'dispatch';
  readonly template: string;
  readonly handlers: number;
  readonly routed?: boolean;
}

export interface MixedCell extends CellBase, Windows, DispatchSettings {
  readonly kind: 'mixed';
  readonly rate: number;
  readonly pubProcs: number;
  readonly pubPool: number;
  readonly maxInflight: number;
  readonly recoveryMaxMs: number;
}

export type Cell = PublishCell | DispatchCell | MixedCell;

export interface Plan {
  readonly mode: Mode;
  readonly pairs: number;
  readonly cells: readonly Cell[];
  readonly templates: readonly TemplateDefinition[];
}

const DEFAULT_POOL: Record<Store, number> = { pg: 10, mongo: 100 };
const DEFAULTS = { concurrency: 10, batchSize: 10, pollMs: 1000 } as const;
const MID = { concurrency: 50, batchSize: 200, pool: 20, pollMs: 1000 } as const;
const TUNED = { concurrency: 200, batchSize: 200, pool: 40, pollMs: 50 } as const;

type SettingsName = 'defaults' | 'mid' | 'tuned';

function settings(store: Store, name: SettingsName, instances: number): DispatchSettings {
  if (name === 'defaults') return { ...DEFAULTS, pool: DEFAULT_POOL[store], instances };
  return { ...(name === 'mid' ? MID : TUNED), instances };
}

function publishCell(
  store: Store,
  id: string,
  title: string,
  windows: Windows,
  reps: number,
  spec: Partial<Omit<PublishCell, 'kind'>>,
): PublishCell {
  return {
    kind: 'publish',
    id: `${store}-pub-${id}`,
    store,
    title,
    reps,
    ...windows,
    procs: 1,
    loops: 8,
    batch: 1,
    handlers: 1,
    pool: DEFAULT_POOL[store],
    keys: NO_KEYS,
    ...spec,
  };
}

function dispatchCell(
  store: Store,
  id: string,
  title: string,
  windows: Windows,
  reps: number,
  template: string,
  handlers: number,
  name: SettingsName,
  instances: number,
  routed = false,
): DispatchCell {
  return {
    kind: 'dispatch',
    id: `${store}-disp-${id}`,
    store,
    title,
    reps,
    ...windows,
    template,
    handlers,
    ...(routed ? { routed } : {}),
    ...settings(store, name, instances),
  };
}

function template(
  store: Store,
  name: string,
  events: number,
  handlers: number,
  keys: KeySpec,
  batch: number,
  routed = false,
): TemplateDefinition {
  return {
    name: `${store}-${name}`,
    store,
    events,
    handlers,
    keys,
    batch,
    procs: 2,
    loops: 4,
    pool: 10,
    ...(routed ? { routed } : {}),
  };
}

function mixedCell(
  store: Store,
  rate: number,
  name: 'defaults' | 'tuned',
  windows: Windows,
  reps: number,
): MixedCell {
  const tuned = name === 'tuned';
  return {
    kind: 'mixed',
    id: `${store}-mixed-${rate}-${name}`,
    store,
    title: `mixed ${rate}/s, ${name} dispatchers`,
    reps,
    ...windows,
    rate,
    pubProcs: tuned ? 4 : 2,
    pubPool: tuned ? 16 : 10,
    maxInflight: 256,
    recoveryMaxMs: 20_000,
    ...(tuned
      ? { instances: 4, concurrency: 100, batchSize: 100, pool: 30, pollMs: 100 }
      : { instances: 1, ...DEFAULTS, pool: DEFAULT_POOL[store] }),
  };
}

/**
 * Quick gate: eight cells, fixed windows. Publish cells report events per second inside the window; dispatch cells
 * clone a preloaded database and count deliveries handled inside the window, so no run waits for a drain.
 */
export function quickPlan(pairs = 3): Plan {
  const publishWindow: Windows = { warmupMs: 1_500, durationMs: 5_000 };
  const window: Windows = { warmupMs: 2_000, durationMs: 6_000 };
  return {
    mode: 'quick',
    pairs,
    templates: [
      template('pg', 'unkeyed', 150_000, 1, NO_KEYS, 500),
      template('pg', 'hot10', 20_000, 1, { mode: 'hot', count: 10 }, 100),
      template('mongo', 'unkeyed', 50_000, 1, NO_KEYS, 500),
    ],
    cells: [
      publishCell('pg', 'p8-b1', 'publish P=8 batch=1', publishWindow, pairs, {}),
      publishCell('pg', 'p8-b100', 'publish P=8 batch=100', publishWindow, pairs, { batch: 100 }),
      publishCell('pg', 'p8-b100-h5', 'publish P=8 batch=100, 5 handlers', publishWindow, pairs, {
        batch: 100,
        handlers: 5,
      }),
      publishCell('mongo', 'p8-b100', 'publish P=8 batch=100', publishWindow, pairs, {
        batch: 100,
      }),
      dispatchCell(
        'pg',
        '1i-tuned',
        'dispatch 1 instance, tuned',
        window,
        pairs,
        'pg-unkeyed',
        1,
        'tuned',
        1,
      ),
      dispatchCell(
        'pg',
        '4i-tuned',
        'dispatch 4 instances, tuned',
        window,
        pairs,
        'pg-unkeyed',
        1,
        'tuned',
        4,
      ),
      dispatchCell(
        'pg',
        'hot10-1i-tuned',
        'keyed dispatch hot:10, 1 instance, tuned',
        window,
        pairs,
        'pg-hot10',
        1,
        'tuned',
        1,
      ),
      dispatchCell(
        'mongo',
        '1i-tuned',
        'dispatch 1 instance, tuned',
        window,
        pairs,
        'mongo-unkeyed',
        1,
        'tuned',
        1,
      ),
    ],
  };
}

/** Full run: about 40 cells with 15 s windows, one sample each except the cells that carry the headline claims. */
export function fullPlan(pairs?: number): Plan {
  const window: Windows = { warmupMs: 3_000, durationMs: 15_000 };
  const mixedWindow: Windows = { warmupMs: 4_000, durationMs: 20_000 };
  const key = pairs ?? 3;
  const single = pairs ?? 1;
  const keyed = (mode: 'distinct' | 'hot', count: number): KeySpec => ({ mode, count });
  const cells: Cell[] = [];
  for (const publishers of [1, 8, 32]) {
    for (const batch of [1, 100]) {
      const headline = publishers === 8 && batch === 100;
      cells.push(
        publishCell(
          'pg',
          `p${publishers}-b${batch}`,
          `publish P=${publishers} batch=${batch}`,
          window,
          headline ? key : single,
          { loops: publishers, batch, pool: Math.max(10, publishers + 2) },
        ),
      );
    }
  }
  cells.push(
    publishCell('pg', 'ceiling-b100', 'publish ceiling 4 procs x 8 batch=100', window, single, {
      procs: 4,
      loops: 8,
      batch: 100,
      pool: 12,
    }),
    publishCell('pg', 'p8-b1-h5', 'publish P=8 batch=1, 5 handlers', window, single, {
      handlers: 5,
    }),
    publishCell('pg', 'p8-b100-h5', 'publish P=8 batch=100, 5 handlers', window, key, {
      batch: 100,
      handlers: 5,
    }),
    publishCell(
      'pg',
      'p8-b1-distinct1000',
      'publish P=8 batch=1 keys distinct:1000',
      window,
      single,
      { keys: keyed('distinct', 1000) },
    ),
    publishCell('pg', 'p8-b1-hot10', 'publish P=8 batch=1 keys hot:10', window, single, {
      keys: keyed('hot', 10),
    }),
    publishCell('pg', 'p8-b1-hot1', 'publish P=8 batch=1 keys hot:1', window, single, {
      keys: keyed('hot', 1),
    }),
    publishCell('mongo', 'p8-b1', 'publish P=8 batch=1', window, single, {}),
    publishCell('mongo', 'p8-b100', 'publish P=8 batch=100', window, key, { batch: 100 }),
    publishCell('mongo', 'p32-b100', 'publish P=32 batch=100', window, single, {
      loops: 32,
      batch: 100,
    }),
    publishCell('mongo', 'p8-b100-h5', 'publish P=8 batch=100, 5 handlers', window, single, {
      batch: 100,
      handlers: 5,
    }),
    publishCell('mongo', 'p8-b1-hot10', 'publish P=8 batch=1 keys hot:10', window, single, {
      keys: keyed('hot', 10),
    }),
  );
  for (const instances of [1, 4, 8]) {
    for (const name of ['defaults', 'mid', 'tuned'] as const) {
      const headline = instances === 1 && name === 'tuned';
      cells.push(
        dispatchCell(
          'pg',
          `${instances}i-${name}`,
          `dispatch ${instances} instances, ${name}`,
          window,
          headline ? key : single,
          'pg-unkeyed',
          1,
          name,
          instances,
        ),
      );
    }
  }
  cells.push(
    dispatchCell(
      'pg',
      'fanout5-1i-tuned',
      'fan-out dispatch 5 handlers, 1 instance, tuned',
      window,
      single,
      'pg-fanout',
      5,
      'tuned',
      1,
    ),
    dispatchCell(
      'pg',
      'fanout5-4i-tuned',
      'fan-out dispatch 5 handlers, 4 instances, tuned',
      window,
      single,
      'pg-fanout',
      5,
      'tuned',
      4,
    ),
    dispatchCell(
      'pg',
      'k1000-1i-tuned',
      'keyed dispatch distinct:1000, 1 instance, tuned',
      window,
      single,
      'pg-k1000',
      1,
      'tuned',
      1,
    ),
    dispatchCell(
      'pg',
      'k1000-4i-tuned',
      'keyed dispatch distinct:1000, 4 instances, tuned',
      window,
      single,
      'pg-k1000',
      1,
      'tuned',
      4,
    ),
    dispatchCell(
      'pg',
      'hot10-1i-tuned',
      'keyed dispatch hot:10, 1 instance, tuned',
      window,
      single,
      'pg-hot10',
      1,
      'tuned',
      1,
    ),
    // Routed: one route to an in-process fake transport that acks after ROUTED_ACK_MS, so no broker is involved.
    dispatchCell(
      'pg',
      'routed-k1000-1i-tuned',
      `routed dispatch distinct:1000, ${ROUTED_ACK_MS} ms ack, 1 instance, tuned`,
      window,
      single,
      'pg-routed-k1000',
      1,
      'tuned',
      1,
      true,
    ),
    dispatchCell(
      'pg',
      'routed-hot10-1i-tuned',
      `routed dispatch hot:10, ${ROUTED_ACK_MS} ms ack, 1 instance, tuned`,
      window,
      single,
      'pg-routed-hot10',
      1,
      'tuned',
      1,
      true,
    ),
  );
  for (const instances of [1, 4]) {
    for (const name of ['defaults', 'tuned'] as const) {
      const headline = instances === 1 && name === 'tuned';
      cells.push(
        dispatchCell(
          'mongo',
          `${instances}i-${name}`,
          `dispatch ${instances} instances, ${name}`,
          window,
          headline ? key : single,
          'mongo-unkeyed',
          1,
          name,
          instances,
        ),
      );
    }
  }
  cells.push(
    dispatchCell(
      'mongo',
      'fanout5-1i-tuned',
      'fan-out dispatch 5 handlers, 1 instance, tuned',
      window,
      single,
      'mongo-fanout',
      5,
      'tuned',
      1,
    ),
    dispatchCell(
      'mongo',
      'k1000-1i-tuned',
      'keyed dispatch distinct:1000, 1 instance, tuned',
      window,
      single,
      'mongo-k1000',
      1,
      'tuned',
      1,
    ),
    dispatchCell(
      'mongo',
      'hot10-1i-tuned',
      'keyed dispatch hot:10, 1 instance, tuned',
      window,
      single,
      'mongo-hot10',
      1,
      'tuned',
      1,
    ),
    mixedCell('pg', 1000, 'defaults', mixedWindow, single),
    mixedCell('pg', 1000, 'tuned', mixedWindow, single),
    mixedCell('pg', 5000, 'tuned', mixedWindow, single),
    mixedCell('mongo', 1000, 'tuned', mixedWindow, single),
    mixedCell('mongo', 5000, 'tuned', mixedWindow, single),
  );
  return {
    mode: 'full',
    pairs: key,
    templates: [
      template('pg', 'unkeyed', 250_000, 1, NO_KEYS, 500),
      template('pg', 'fanout', 50_000, 5, NO_KEYS, 500),
      template('pg', 'k1000', 120_000, 1, keyed('distinct', 1000), 100),
      template('pg', 'hot10', 40_000, 1, keyed('hot', 10), 100),
      template('pg', 'routed-k1000', 120_000, 1, keyed('distinct', 1000), 100, true),
      template('pg', 'routed-hot10', 40_000, 1, keyed('hot', 10), 100, true),
      template('mongo', 'unkeyed', 100_000, 1, NO_KEYS, 500),
      template('mongo', 'fanout', 20_000, 5, NO_KEYS, 100),
      template('mongo', 'k1000', 40_000, 1, keyed('distinct', 1000), 100),
      template('mongo', 'hot10', 15_000, 1, keyed('hot', 10), 100),
    ],
    cells,
  };
}

/** Restricts a plan to the cells whose id contains one of the given fragments (and the templates they need). */
export function filterPlan(plan: Plan, fragments: readonly string[]): Plan {
  const cells = plan.cells.filter((cell) =>
    fragments.some((fragment) => cell.id.includes(fragment)),
  );
  const needed = new Set(
    cells.flatMap((cell) => (cell.kind === 'dispatch' ? [cell.template] : [])),
  );
  return {
    ...plan,
    cells,
    templates: plan.templates.filter((candidate) => needed.has(candidate.name)),
  };
}

/** Everything about the plan that changes what is measured (not how often); part of the baseline fingerprint. */
export function planDescriptor(plan: Plan): unknown {
  return {
    mode: plan.mode,
    templates: plan.templates,
    cells: plan.cells.map((cell) =>
      Object.fromEntries(Object.entries(cell).filter(([name]) => name !== 'reps')),
    ),
  };
}
