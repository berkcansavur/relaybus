import { execFile, type ChildProcess } from 'node:child_process';

export interface DockerSummary {
  readonly cpuAvgPct: number | null;
  readonly cpuMaxPct: number | null;
  readonly memMaxMiB: number | null;
  readonly samples: number;
}

/** Converts `docker stats` memory text such as `1.2GiB` or `512MiB` to mebibytes. */
export function parseMemoryMiB(text: string): number {
  const match = /([\d.]+)\s*(KiB|MiB|GiB|B)/.exec(text);
  if (match === null) return Number.NaN;
  const value = parseFloat(match[1] ?? '');
  const unit = match[2];
  if (unit === 'GiB') return value * 1024;
  if (unit === 'KiB') return value / 1024;
  if (unit === 'B') return value / 1_048_576;
  return value;
}

/** Samples `docker stats` of one container in the background (each call takes 1-2 s on Docker Desktop). */
export class DockerSampler {
  readonly #containerId: string;
  readonly #cpu: number[] = [];
  readonly #memory: number[] = [];
  #running = false;
  #loop: Promise<void> | undefined;
  #child: ChildProcess | undefined;

  constructor(containerId: string) {
    this.#containerId = containerId;
  }

  start(): void {
    this.#cpu.length = 0;
    this.#memory.length = 0;
    this.#running = true;
    this.#loop = this.#poll();
  }

  async stop(): Promise<DockerSummary> {
    this.#running = false;
    // A call to `docker stats` takes 1-2 s; the sample in flight is abandoned instead of waited for.
    this.#child?.kill();
    await this.#loop;
    if (this.#cpu.length === 0)
      return { cpuAvgPct: null, cpuMaxPct: null, memMaxMiB: null, samples: 0 };
    return {
      cpuAvgPct: Math.round(this.#cpu.reduce((a, b) => a + b, 0) / this.#cpu.length),
      cpuMaxPct: Math.round(Math.max(...this.#cpu)),
      memMaxMiB: Math.round(Math.max(...this.#memory, 0)),
      samples: this.#cpu.length,
    };
  }

  async #poll(): Promise<void> {
    while (this.#running) {
      await new Promise<void>((resolve) => {
        this.#child = execFile(
          'docker',
          ['stats', '--no-stream', '--format', '{{.CPUPerc}}|{{.MemUsage}}', this.#containerId],
          (error, stdout) => {
            if (error === null) {
              const [cpuText = '', memoryText = ''] = stdout.trim().split('|');
              const cpu = parseFloat(cpuText);
              const memory = parseMemoryMiB(memoryText.split('/')[0]?.trim() ?? '');
              if (Number.isFinite(cpu)) this.#cpu.push(cpu);
              if (Number.isFinite(memory)) this.#memory.push(memory);
            }
            resolve();
          },
        );
      });
    }
  }
}
