import { fork, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const nowMs = (): number => performance.timeOrigin + performance.now();
export const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
export const sleepUntil = (epochMs: number): Promise<void> => sleep(Math.max(0, epochMs - nowMs()));

const WORKER = fileURLToPath(new URL('../worker.ts', import.meta.url));

export type Listener = (message: any, index: number) => void;

/** A set of worker processes that are initialised, started together and collected together. */
export class Fleet {
  readonly workers: ChildProcess[] = [];
  readonly results: any[] = [];
  #ready: Promise<void>[] = [];
  #done: Promise<void>[] = [];
  #failure: ((error: Error) => void) | undefined;
  readonly failed: Promise<never>;
  readonly lastProgress: number[] = [];

  constructor(inits: unknown[], onMessage?: Listener) {
    this.failed = new Promise<never>((_, reject) => (this.#failure = reject));
    this.failed.catch(() => undefined);
    inits.forEach((init, index) => {
      const child = fork(WORKER, [], { stdio: ['ignore', 'inherit', 'inherit', 'ipc'], serialization: 'json' });
      this.workers.push(child);
      this.lastProgress.push(0);
      this.#ready.push(
        new Promise((resolve) => {
          child.on('message', (message: any) => {
            if (message.t === 'ready') resolve();
          });
        }),
      );
      this.#done.push(
        new Promise((resolve) => {
          child.on('message', (message: any) => {
            if (message.t === 'done') {
              this.results[index] = message.result;
              resolve();
            }
          });
        }),
      );
      child.on('message', (message: any) => {
        if (message.t === 'error') this.#failure?.(new Error(`worker ${index}: ${message.message}`));
        if (message.t === 'progress') this.lastProgress[index] = message.handled;
        onMessage?.(message, index);
      });
      child.on('exit', (code) => {
        if (code !== 0 && code !== null && this.results[index] === undefined) {
          this.#failure?.(new Error(`worker ${index} exited with code ${code}`));
        }
      });
      child.send({ cmd: 'init', ...(init as object) });
    });
  }

  async ready(): Promise<void> {
    await Promise.race([Promise.all(this.#ready), this.failed]);
  }

  go(startAt: number): void {
    for (const worker of this.workers) worker.send({ cmd: 'go', startAt });
  }

  stop(): void {
    for (const worker of this.workers) if (worker.connected) worker.send({ cmd: 'stop' });
  }

  async done(): Promise<any[]> {
    await Promise.race([Promise.all(this.#done), this.failed]);
    return this.results;
  }

  kill(): void {
    for (const worker of this.workers) worker.kill('SIGKILL');
  }

  get handledTotal(): number {
    return this.lastProgress.reduce((a, b) => a + b, 0);
  }
}
