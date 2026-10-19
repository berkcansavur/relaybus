// One benchmark process: a publisher / loader or a dispatcher instance with its own pool and RelayBus.
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { core, pg, postgresPkg, mongodb, mongoPkg } from './lib/deps.ts';
import { Hist } from './lib/hist.ts';

const nowMs = (): number => performance.timeOrigin + performance.now();
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
const send = (message: unknown): void => void process.send!(message);

type Payload = { w: number; n: number; orderId: string; customerId: string; amount: number; note: string };
class Bench extends core.defineEvent<Payload>('bench.order.placed.v1') {}

const PAD = 'x'.repeat(160);
let init: any;
let bus: any;
let pool: any;
let client: any;
let loopDelay: any;
let stopping = false;

process.on('message', (message: any) => {
  if (message.cmd === 'init') void setup(message).catch(fail);
  else if (message.cmd === 'go') void run(message.startAt).catch(fail);
  else if (message.cmd === 'stop') stopping = true;
});

function fail(error: unknown): void {
  send({ t: 'error', message: error instanceof Error ? (error.stack ?? error.message) : String(error) });
}

async function setup(message: any): Promise<void> {
  init = message;
  const { backend, url, database, cfg } = init;
  let store: any;
  if (backend === 'pg') {
    pool = new pg.Pool({ connectionString: url, max: cfg.pool ?? 10 });
    pool.on('error', () => undefined);
    store = new postgresPkg.PostgresEventStore({ pool });
  } else {
    client = await new mongodb.MongoClient(url, {
      directConnection: true,
      maxPoolSize: cfg.pool ?? 100,
    }).connect();
    store = new mongoPkg.MongoEventStore({ client, database });
  }
  const failures: string[] = [];
  init.failures = failures;
  const observer = {
    onDeliverySucceeded: () => void (init.handled = (init.handled ?? 0) + 1, (init.lastAt = nowMs())),
    onFailure: ({ stage, error }: any) => void failures.push(`${stage}:${error.name}:${error.message}`.slice(0, 200)),
    onLeaseLost: () => void (failures.push('lease-lost'), (init.leaseLost = (init.leaseLost ?? 0) + 1)),
  };
  const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };
  const isDispatcher = init.role === 'dispatcher';
  bus = new core.RelayBus({
    store,
    source: 'bench',
    dispatcher: isDispatcher
      ? {
          enabled: true,
          ...(cfg.concurrency === undefined ? {} : { concurrency: cfg.concurrency }),
          ...(cfg.batchSize === undefined ? {} : { batchSize: cfg.batchSize }),
          ...(cfg.pollMs === undefined ? {} : { pollInterval: cfg.pollMs }),
        }
      : { enabled: false },
    retention: { completed: false, dead: false },
    observers: [observer],
    logger: silent,
  });
  const tracked: Array<[number, number, number]> = [];
  init.tracked = tracked;
  for (let index = 1; index <= cfg.handlers; index += 1) {
    const handlerNumber = index;
    bus.subscribe(
      Bench,
      cfg.trackHandled
        ? (event: any) => void tracked.push([event.payload.w, event.payload.n * 16 + handlerNumber, nowMs()])
        : () => undefined,
      { id: `h${index}` },
    );
  }
  loopDelay = monitorEventLoopDelay({ resolution: 5 });
  send({ t: 'ready' });
}

async function run(startAt: number): Promise<void> {
  if (init.role === 'dispatcher') await runDispatcher(startAt);
  else await runPublisher(startAt);
}

async function runDispatcher(startAt: number): Promise<void> {
  const { cfg } = init;
  await sleep(Math.max(0, startAt - nowMs()));
  const cpu0 = process.cpuUsage();
  loopDelay.enable();
  const began = nowMs();
  await bus.start();
  const flush = setInterval(() => {
    send({ t: 'progress', handled: init.handled ?? 0 });
    if (cfg.trackHandled && init.tracked.length > 0) send({ t: 'handled', items: init.tracked.splice(0) });
  }, 200);
  while (!stopping) await sleep(50);
  clearInterval(flush);
  loopDelay.disable();
  const cpu = process.cpuUsage(cpu0);
  await bus.stop();
  // Handlers that finished during the shutdown wait must be reported too, or the exactness check would count them as lost.
  if (cfg.trackHandled && init.tracked.length > 0) send({ t: 'handled', items: init.tracked.splice(0) });
  send({
    t: 'done',
    result: {
      handled: init.handled ?? 0,
      began,
      lastAt: init.lastAt ?? began,
      cpuMs: (cpu.user + cpu.system) / 1000,
      loopDelayP99: loopDelay.percentile(99) / 1e6,
      failures: init.failures.slice(0, 5),
      failureCount: init.failures.length,
      leaseLost: init.leaseLost ?? 0,
    },
  });
  await closeClients();
}

async function runPublisher(startAt: number): Promise<void> {
  const { cfg } = init;
  const measureStart = startAt + cfg.warmupMs;
  const measureEnd = measureStart + cfg.durationMs;
  const pubHist = new Hist();
  const txHist = new Hist();
  const commits: Array<[number, number]> = [];
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

  async function publishOnce(): Promise<void> {
    const batch: number = cfg.batch;
    const batchEvents = [];
    const firstN = counter;
    for (let i = 0; i < batch; i += 1) {
      const n = counter++;
      batchEvents.push(
        new Bench({ w: cfg.w, n, orderId: `o-${cfg.w}-${n}`, customerId: `c-${n % 5000}`, amount: n % 997, note: PAD }),
      );
    }
    const key = pickKey();
    const t0 = nowMs();
    let publishMs = 0;
    try {
      await bus.transaction(async (transaction: unknown) => {
        const t1 = nowMs();
        if (batch === 1) await bus.publish(batchEvents[0], { transaction, ...(key === undefined ? {} : { key }) });
        else await bus.publishAll(batchEvents, { transaction });
        publishMs = nowMs() - t1;
      });
    } catch (error) {
      errors += 1;
      if (errorSamples.length < 3) errorSamples.push(String((error as Error)?.message ?? error).slice(0, 200));
      return;
    }
    const t2 = nowMs();
    if (cfg.trackCommits) for (let i = 0; i < batch; i += 1) commits.push([firstN + i, t2]);
    if (t2 >= measureStart && t2 < measureEnd) {
      pubHist.add(publishMs);
      txHist.add(t2 - t0);
      events += batch;
      txs += 1;
    }
  }

  const flush = setInterval(() => {
    if (commits.length > 0) send({ t: 'commits', w: cfg.w, items: commits.splice(0) });
  }, 200);

  await sleep(Math.max(0, startAt - nowMs()));
  loopDelay.enable();
  let cpu0 = process.cpuUsage();
  const cpuTimer = setTimeout(() => (cpu0 = process.cpuUsage()), Math.max(0, measureStart - nowMs()));
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
  loopDelay.disable();
  const cpu = process.cpuUsage(cpu0);
  if (commits.length > 0) send({ t: 'commits', w: cfg.w, items: commits.splice(0) });
  await bus.stop();
  send({
    t: 'done',
    result: {
      events,
      txs,
      errors,
      dropped,
      errorSamples,
      totalPublished: counter,
      pubHist: pubHist.toJSON(),
      txHist: txHist.toJSON(),
      cpuMs: (cpu.user + cpu.system) / 1000,
      loopDelayP99: loopDelay.percentile(99) / 1e6,
    },
  });
  await closeClients();
}

async function closeClients(): Promise<void> {
  await pool?.end().catch(() => undefined);
  await client?.close().catch(() => undefined);
}
