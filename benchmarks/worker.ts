// One benchmark process: a publisher / loader or a dispatcher instance with its own pool and RelayBus.
// It loads the packages of the side named by BENCH_SIDE_ROOT, so a base and a candidate checkout run the same worker code.
import { monitorEventLoopDelay, type IntervalHistogram } from 'node:perf_hooks';
import type { EventStore, RelayBus, RelayBusObserver, Transport } from '@relaybus/core';
import type { Pool } from 'pg';
import type { MongoClient } from 'mongodb';
import { loadSide } from './lib/deps.ts';
import { Hist } from './lib/hist.ts';
import type { CommitItem, FromWorker, HandledItem, ToWorker, WorkerInit } from './lib/protocol.ts';

const nowMs = (): number => performance.timeOrigin + performance.now();
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const send = (message: FromWorker): void => void process.send?.(message);

const sideRoot = process.env['BENCH_SIDE_ROOT'];
if (sideRoot === undefined) throw new Error('[worker](startup) BENCH_SIDE_ROOT is not set');
const side = await loadSide(sideRoot);

interface Payload {
  w: number;
  n: number;
  orderId: string;
  customerId: string;
  amount: number;
  note: string;
}
class Bench extends side.core.defineEvent<Payload>('bench.order.placed.v1') {}

const PAD = 'x'.repeat(160);

/**
 * Stands in for a broker: acks a send after a fixed delay and reports the send to the consumer side, which is what the
 * exactness and per-key order checks read. Both releases accept it unchanged (`name`, `publish`, `close`).
 */
function fakeTransport(
  ackMs: number,
  report: ((w: number, n: number) => void) | undefined,
): Transport {
  return {
    name: 'bench-fake',
    publish: async (envelope) => {
      const payload = envelope.payload as Payload;
      report?.(payload.w, payload.n);
      await sleep(ackMs);
    },
    close: async () => undefined,
  };
}

/** The part of RelayBus the worker uses, independent of the store's transaction type. */
interface BenchBus {
  start(): Promise<void>;
  stop(): Promise<void>;
  subscribe(id: string, handler: (event: Bench) => void): void;
  /** Publishes the events in one transaction and returns the time the publish call took inside it. */
  publishInTransaction(events: readonly Bench[], key: string | undefined): Promise<number>;
}

function adapt<TTransaction>(
  store: EventStore<TTransaction>,
  options: {
    dispatcher: {
      enabled: boolean;
      concurrency?: number;
      batchSize?: number;
      pollInterval?: number;
    };
    observer: RelayBusObserver;
    /** Route every event to a fake transport instead of registering local handlers. */
    transport?: Transport;
  },
): BenchBus {
  const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };
  const bus: RelayBus<TTransaction> = new side.core.RelayBus({
    store,
    source: 'bench',
    dispatcher: options.dispatcher,
    retention: { completed: false, dead: false },
    observers: [options.observer],
    logger: silent,
    ...(options.transport === undefined
      ? {}
      : {
          transports: [options.transport],
          routes: [{ event: Bench, transport: options.transport.name, destination: 'bench' }],
        }),
  });
  return {
    start: () => bus.start(),
    stop: async () => {
      await bus.stop();
    },
    subscribe: (id, handler) => void bus.subscribe(Bench, handler, { id }),
    publishInTransaction: (events, key) =>
      bus.transaction(async (transaction) => {
        const started = nowMs();
        const keyOption = key === undefined ? {} : { key };
        const first = events[0];
        if (events.length === 1 && first !== undefined)
          await bus.publish(first, { transaction, ...keyOption });
        else await bus.publishAll([...events], { transaction, ...keyOption });
        return nowMs() - started;
      }),
  };
}

let init: WorkerInit | undefined;
let bus: BenchBus | undefined;
let pool: Pool | undefined;
let client: MongoClient | undefined;
let loopDelay: IntervalHistogram | undefined;
let stopping = false;
let handledCount = 0;
let lastHandledAt = 0;
let leaseLost = 0;
const failures: string[] = [];
const tracked: HandledItem[] = [];

process.on('message', (message: ToWorker) => {
  if (message.cmd === 'init') void setup(message).catch(fail);
  else if (message.cmd === 'go') void run(message.startAt).catch(fail);
  else stopping = true;
});

function fail(error: unknown): void {
  send({
    t: 'error',
    message: error instanceof Error ? (error.stack ?? error.message) : String(error),
  });
}

async function setup(message: WorkerInit): Promise<void> {
  init = message;
  const { backend, url, database, cfg } = message;
  const observer: RelayBusObserver = {
    onDeliverySucceeded: () => {
      handledCount += 1;
      lastHandledAt = nowMs();
    },
    onFailure: ({ stage, error }) =>
      void failures.push(`${stage}:${error.name}:${error.message}`.slice(0, 200)),
    onLeaseLost: () => {
      failures.push('lease-lost');
      leaseLost += 1;
    },
  };
  const dispatcher =
    message.role === 'dispatcher'
      ? {
          enabled: true,
          concurrency: message.cfg.concurrency,
          batchSize: message.cfg.batchSize,
          pollInterval: message.cfg.pollMs,
        }
      : { enabled: false };
  const trackHandled = message.role === 'dispatcher' && message.cfg.trackHandled;
  const routed = cfg.routeAckMs !== undefined;
  const transport = routed
    ? fakeTransport(
        cfg.routeAckMs ?? 0,
        trackHandled ? (w, n) => void tracked.push([w, n * 16 + 1, nowMs()]) : undefined,
      )
    : undefined;
  if (backend === 'pg') {
    pool = new side.pg.Pool({ connectionString: url, max: cfg.pool });
    pool.on('error', () => undefined);
    bus = adapt(new side.postgres.PostgresEventStore({ pool }), {
      dispatcher,
      observer,
      ...(transport === undefined ? {} : { transport }),
    });
  } else {
    client = await new side.mongoDriver.MongoClient(url, {
      directConnection: true,
      maxPoolSize: cfg.pool,
    }).connect();
    bus = adapt(new side.mongoPackage.MongoEventStore({ client, database }), {
      dispatcher,
      observer,
      ...(transport === undefined ? {} : { transport }),
    });
  }
  // Publishers register the handlers too, so that the store writes one delivery per handler (a route replaces them).
  for (let handlerNumber = 1; !routed && handlerNumber <= cfg.handlers; handlerNumber += 1) {
    bus.subscribe(`h${handlerNumber}`, (event) => {
      if (trackHandled)
        tracked.push([event.payload.w, event.payload.n * 16 + handlerNumber, nowMs()]);
    });
  }
  loopDelay = monitorEventLoopDelay({ resolution: 5 });
  send({ t: 'ready' });
}

async function closeClients(): Promise<void> {
  await pool?.end().catch(() => undefined);
  await client?.close().catch(() => undefined);
}

async function run(startAt: number): Promise<void> {
  if (init?.role === 'dispatcher') await runDispatcher(startAt);
  else if (init?.role === 'publisher') await runPublisher(startAt, init.cfg);
}

async function runDispatcher(startAt: number): Promise<void> {
  const activeBus = bus;
  const activeLoopDelay = loopDelay;
  if (activeBus === undefined || activeLoopDelay === undefined || init?.role !== 'dispatcher')
    return;
  const { trackHandled } = init.cfg;
  await sleep(Math.max(0, startAt - nowMs()));
  const cpuStart = process.cpuUsage();
  activeLoopDelay.enable();
  const began = nowMs();
  await activeBus.start();
  const flush = setInterval(() => {
    send({ t: 'progress', handled: handledCount });
    if (trackHandled && tracked.length > 0) send({ t: 'handled', items: tracked.splice(0) });
  }, 200);
  while (!stopping) await sleep(50);
  clearInterval(flush);
  activeLoopDelay.disable();
  const cpu = process.cpuUsage(cpuStart);
  await activeBus.stop();
  // Handlers that finished during the shutdown wait must be reported too, or the exactness check would count them as lost.
  if (trackHandled && tracked.length > 0) send({ t: 'handled', items: tracked.splice(0) });
  await closeClients();
  send({
    t: 'done',
    result: {
      kind: 'dispatcher',
      handled: handledCount,
      lastAt: handledCount > 0 ? lastHandledAt : began,
      cpuMs: (cpu.user + cpu.system) / 1000,
      loopDelayP99: activeLoopDelay.percentile(99) / 1e6,
      failures: failures.slice(0, 5),
      failureCount: failures.length,
      leaseLost,
    },
  });
}

async function runPublisher(
  startAt: number,
  cfg: Extract<WorkerInit, { role: 'publisher' }>['cfg'],
): Promise<void> {
  const activeBus = bus;
  const activeLoopDelay = loopDelay;
  if (activeBus === undefined || activeLoopDelay === undefined) return;
  const measureStart = startAt + cfg.warmupMs;
  const measureEnd = measureStart + cfg.durationMs;
  const pubHist = new Hist();
  const txHist = new Hist();
  const commits: CommitItem[] = [];
  let counter = 0;
  let events = 0;
  let txs = 0;
  let errors = 0;
  let dropped = 0;
  let inflight = 0;
  let issuedEvents = 0;
  const errorSamples: string[] = [];

  const pickKey = (): string | undefined =>
    cfg.keys.mode === 'none' ? undefined : `k${Math.floor(Math.random() * cfg.keys.count)}`;

  const publishOnce = async (): Promise<void> => {
    const batchEvents: Bench[] = [];
    const firstN = counter;
    for (let index = 0; index < cfg.batch; index += 1) {
      const n = counter++;
      batchEvents.push(
        new Bench({
          w: cfg.w,
          n,
          orderId: `o-${cfg.w}-${n}`,
          customerId: `c-${n % 5000}`,
          amount: n % 997,
          note: PAD,
        }),
      );
    }
    const startedAt = nowMs();
    let publishMs = 0;
    try {
      publishMs = await activeBus.publishInTransaction(batchEvents, pickKey());
    } catch (error) {
      errors += 1;
      if (errorSamples.length < 3)
        errorSamples.push((error instanceof Error ? error.message : String(error)).slice(0, 200));
      return;
    }
    const committedAt = nowMs();
    if (cfg.trackCommits)
      for (let index = 0; index < cfg.batch; index += 1)
        commits.push([firstN + index, committedAt]);
    if (committedAt >= measureStart && committedAt < measureEnd) {
      pubHist.add(publishMs);
      txHist.add(committedAt - startedAt);
      events += cfg.batch;
      txs += 1;
    }
  };

  const flush = setInterval(() => {
    if (commits.length > 0) send({ t: 'commits', w: cfg.w, items: commits.splice(0) });
  }, 200);

  await sleep(Math.max(0, startAt - nowMs()));
  activeLoopDelay.enable();
  let cpuStart = process.cpuUsage();
  const cpuTimer = setTimeout(
    () => (cpuStart = process.cpuUsage()),
    Math.max(0, measureStart - nowMs()),
  );
  const finished = (): boolean => stopping || nowMs() >= measureEnd;

  if (cfg.rate > 0) {
    // Open loop: a fixed offered rate; work beyond maxInflight is shed and counted.
    let issuedTx = 0;
    const txPerMs = cfg.rate / cfg.batch / 1000;
    while (!finished()) {
      const due = Math.floor((nowMs() - startAt) * txPerMs);
      while (issuedTx < due) {
        issuedTx += 1;
        if (inflight >= cfg.maxInflight) {
          dropped += cfg.batch;
          continue;
        }
        inflight += 1;
        void publishOnce().finally(() => (inflight -= 1));
      }
      await sleep(4);
    }
    while (inflight > 0) await sleep(10);
  } else {
    // Closed loop: each loop publishes as soon as its previous transaction committed.
    await Promise.all(
      Array.from({ length: cfg.loops }, async () => {
        while (!finished()) {
          if (cfg.maxEvents !== undefined) {
            if (issuedEvents >= cfg.maxEvents) break;
            issuedEvents += cfg.batch;
          }
          await publishOnce();
        }
      }),
    );
  }
  clearTimeout(cpuTimer);
  clearInterval(flush);
  activeLoopDelay.disable();
  const cpu = process.cpuUsage(cpuStart);
  if (commits.length > 0) send({ t: 'commits', w: cfg.w, items: commits.splice(0) });
  await activeBus.stop();
  await closeClients();
  send({
    t: 'done',
    result: {
      kind: 'publisher',
      events,
      txs,
      errors,
      dropped,
      errorSamples,
      pubHist: pubHist.toJSON(),
      txHist: txHist.toJSON(),
      cpuMs: (cpu.user + cpu.system) / 1000,
      loopDelayP99: activeLoopDelay.percentile(99) / 1e6,
    },
  });
}
