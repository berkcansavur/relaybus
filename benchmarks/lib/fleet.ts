import { fork, type ChildProcess } from 'node:child_process';
import type { FromWorker, ToWorker, WorkerInit, WorkerResult } from './protocol.ts';

export const nowMs = (): number => performance.timeOrigin + performance.now();
export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));
export const sleepUntil = (epochMs: number): Promise<void> => sleep(Math.max(0, epochMs - nowMs()));

const WORKER = new URL('../worker.ts', import.meta.url).pathname;

export type Listener = (message: FromWorker, index: number) => void;

/** A set of worker processes that are initialised, started together and collected together. All of them run one side's dist. */
export class Fleet {
  readonly #workers: ChildProcess[] = [];
  readonly #results: Array<WorkerResult | undefined> = [];
  readonly #ready: Array<Promise<void>> = [];
  readonly #done: Array<Promise<void>> = [];
  readonly #progress: number[] = [];
  readonly #failed: Promise<never>;
  #fail: ((error: Error) => void) | undefined;

  constructor(sideRoot: string, inits: readonly WorkerInit[], onMessage?: Listener) {
    this.#failed = new Promise<never>((_resolve, reject) => {
      this.#fail = reject;
    });
    this.#failed.catch(() => undefined);
    inits.forEach((init, index) => {
      const child = fork(WORKER, [], {
        stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
        serialization: 'json',
        env: { ...process.env, BENCH_SIDE_ROOT: sideRoot },
      });
      this.#workers.push(child);
      this.#progress.push(0);
      let markReady: () => void = () => undefined;
      let markDone: () => void = () => undefined;
      this.#ready.push(new Promise<void>((resolve) => (markReady = resolve)));
      this.#done.push(new Promise<void>((resolve) => (markDone = resolve)));
      child.on('message', (message: FromWorker) => {
        if (message.t === 'ready') markReady();
        if (message.t === 'done') {
          this.#results[index] = message.result;
          markDone();
        }
        if (message.t === 'error') this.#fail?.(new Error(`worker ${index}: ${message.message}`));
        if (message.t === 'progress') this.#progress[index] = message.handled;
        onMessage?.(message, index);
      });
      child.on('exit', (code) => {
        if (code !== 0 && code !== null && this.#results[index] === undefined)
          this.#fail?.(new Error(`worker ${index} exited with code ${code}`));
      });
      this.#send(child, { cmd: 'init', ...init });
    });
  }

  #send(child: ChildProcess, message: ToWorker): void {
    child.send(message);
  }

  async ready(): Promise<void> {
    await Promise.race([Promise.all(this.#ready), this.#failed]);
  }

  go(startAt: number): void {
    for (const worker of this.#workers) this.#send(worker, { cmd: 'go', startAt });
  }

  stop(): void {
    for (const worker of this.#workers) if (worker.connected) this.#send(worker, { cmd: 'stop' });
  }

  async done(): Promise<WorkerResult[]> {
    await Promise.race([Promise.all(this.#done), this.#failed]);
    return this.#results.filter((result): result is WorkerResult => result !== undefined);
  }

  kill(): void {
    for (const worker of this.#workers) worker.kill('SIGKILL');
  }

  /** Handler invocations reported so far by all workers (updated every 200 ms). */
  get handledTotal(): number {
    return this.#progress.reduce((a, b) => a + b, 0);
  }
}
