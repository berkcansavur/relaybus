// Messages between the harness and its worker processes (publishers, loaders and dispatcher instances).
import type { KeySpec, Store } from './cells.ts';
import type { SerializedHist } from './hist.ts';

export interface PublisherConfig {
  /** Worker number; part of the identity of every published event. */
  readonly w: number;
  /** Concurrent closed-loop publishers (ignored when `rate` > 0). */
  readonly loops: number;
  readonly batch: number;
  readonly handlers: number;
  readonly keys: KeySpec;
  /** Seed of the event ids and ordering keys this worker generates; the same seed gives the same database content on every side. */
  readonly seed: number;
  /** Route events to the fake transport instead of registering local handlers (ack delay in ms). */
  readonly routeAckMs?: number;
  readonly pool: number;
  readonly warmupMs: number;
  readonly durationMs: number;
  /** Offered events per second for an open loop; 0 for a closed loop. */
  readonly rate: number;
  readonly maxInflight: number;
  readonly trackCommits: boolean;
  /** Stop after this many events (template loading). */
  readonly maxEvents?: number;
}

export interface DispatcherConfig {
  readonly handlers: number;
  readonly concurrency: number;
  readonly batchSize: number;
  readonly pollMs: number;
  readonly pool: number;
  readonly trackHandled: boolean;
  /** The handler is a route to the fake transport that acks after this many ms. */
  readonly routeAckMs?: number;
}

interface InitBase {
  readonly backend: Store;
  readonly url: string;
  readonly database: string;
}

export type WorkerInit =
  | (InitBase & { readonly role: 'publisher'; readonly cfg: PublisherConfig })
  | (InitBase & { readonly role: 'dispatcher'; readonly cfg: DispatcherConfig });

export type ToWorker =
  | ({ readonly cmd: 'init' } & WorkerInit)
  | { readonly cmd: 'go'; readonly startAt: number }
  | { readonly cmd: 'stop' };

/** `[worker, n * 16 + handlerNumber, timestamp]`: one handler invocation of event `n` published by worker `w`. */
export type HandledItem = readonly [number, number, number];
/** `[n, timestamp]`: event `n` of the publishing worker was committed at the timestamp. */
export type CommitItem = readonly [number, number];

export interface PublisherResult {
  readonly kind: 'publisher';
  readonly events: number;
  readonly txs: number;
  readonly errors: number;
  readonly dropped: number;
  readonly errorSamples: readonly string[];
  readonly pubHist: SerializedHist;
  readonly txHist: SerializedHist;
  readonly cpuMs: number;
  readonly loopDelayP99: number;
}

export interface DispatcherResult {
  readonly kind: 'dispatcher';
  readonly handled: number;
  readonly lastAt: number;
  readonly cpuMs: number;
  readonly loopDelayP99: number;
  readonly failures: readonly string[];
  readonly failureCount: number;
  readonly leaseLost: number;
}

export type WorkerResult = PublisherResult | DispatcherResult;

export type FromWorker =
  | { readonly t: 'ready' }
  | { readonly t: 'error'; readonly message: string }
  | { readonly t: 'progress'; readonly handled: number }
  | { readonly t: 'handled'; readonly items: readonly HandledItem[] }
  | { readonly t: 'commits'; readonly w: number; readonly items: readonly CommitItem[] }
  | { readonly t: 'done'; readonly result: WorkerResult };
