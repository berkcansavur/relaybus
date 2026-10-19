import { execFile } from 'node:child_process';

/** Samples `docker stats` of one container in the background (each call takes ~1-2 s on Docker Desktop). */
export class DockerSampler {
  readonly cpu: number[] = [];
  readonly memMiB: number[] = [];
  #running = false;
  #loop: Promise<void> | undefined;

  readonly containerId: string;

  constructor(containerId: string) {
    this.containerId = containerId;
  }

  start(): void {
    this.cpu.length = 0;
    this.memMiB.length = 0;
    this.#running = true;
    this.#loop = (async () => {
      while (this.#running) {
        await new Promise<void>((resolve) => {
          execFile(
            'docker',
            ['stats', '--no-stream', '--format', '{{.CPUPerc}}|{{.MemUsage}}', this.containerId],
            (error, stdout) => {
              if (!error) {
                const [cpu, mem] = stdout.trim().split('|');
                const cpuValue = parseFloat(cpu ?? '');
                const memValue = parseMem(mem?.split('/')[0]?.trim() ?? '');
                if (Number.isFinite(cpuValue)) this.cpu.push(cpuValue);
                if (Number.isFinite(memValue)) this.memMiB.push(memValue);
              }
              resolve();
            },
          );
        });
      }
    })();
  }

  async stop(): Promise<{ cpuAvgPct: number; cpuMaxPct: number; memMaxMiB: number; samples: number }> {
    this.#running = false;
    await this.#loop;
    const avg = this.cpu.length === 0 ? NaN : this.cpu.reduce((a, b) => a + b, 0) / this.cpu.length;
    return {
      cpuAvgPct: round(avg, 0),
      cpuMaxPct: round(Math.max(...this.cpu, 0), 0),
      memMaxMiB: round(Math.max(...this.memMiB, 0), 0),
      samples: this.cpu.length,
    };
  }
}

function parseMem(text: string): number {
  const match = /([\d.]+)\s*(KiB|MiB|GiB|B)/.exec(text);
  if (!match) return NaN;
  const value = parseFloat(match[1]!);
  return match[2] === 'GiB' ? value * 1024 : match[2] === 'KiB' ? value / 1024 : match[2] === 'B' ? value / 1048576 : value;
}

const round = (value: number, digits: number): number => Math.round(value * 10 ** digits) / 10 ** digits;
