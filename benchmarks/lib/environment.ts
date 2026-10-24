// Facts about the machine and the toolchain. They end up in env.json and, together with the plan, in the fingerprint
// that decides whether two measurements may be compared.
import { execFileSync } from 'node:child_process';
import { PG_IMAGE, PG_SETTINGS } from './pg-backend.ts';
import { MONGO_IMAGE } from './mongo-backend.ts';
import type { Plan } from './cells.ts';
import { planDescriptor } from './cells.ts';
import { fingerprintOf } from './baseline.ts';

/** Bump when the meaning of a measured quantity changes; it invalidates every stored baseline. */
export const HARNESS_VERSION = 2;

function run(command: string, args: readonly string[]): string {
  try {
    return execFileSync(command, args, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
  } catch {
    return 'unavailable';
  }
}

export interface MachineFacts {
  readonly hostCpu: string;
  readonly hostCores: string;
  readonly hostPerfCores: string;
  readonly hostMemGiB: number;
  readonly os: string;
  readonly node: string;
  readonly docker: string;
}

export function collectMachine(): MachineFacts {
  return {
    hostCpu: run('sysctl', ['-n', 'machdep.cpu.brand_string']),
    hostCores: run('sysctl', ['-n', 'hw.ncpu']),
    hostPerfCores: run('sysctl', ['-n', 'hw.perflevel0.logicalcpu']),
    hostMemGiB: Math.round(Number(run('sysctl', ['-n', 'hw.memsize'])) / 2 ** 30),
    os: `${run('uname', ['-sr'])} ${run('sw_vers', ['-productVersion'])}`.trim(),
    node: process.version,
    docker: run('docker', [
      'info',
      '--format',
      '{{.NCPU}} CPUs, {{.MemTotal}} bytes, {{.OperatingSystem}}, server {{.ServerVersion}}',
    ]),
  };
}

/** Local image id of an image, pulling it first when it is missing. */
export function ensureImage(image: string): string {
  const inspect = (): string => run('docker', ['image', 'inspect', '--format', '{{.Id}}', image]);
  const id = inspect();
  if (id !== 'unavailable') return id;
  run('docker', ['pull', image]);
  return inspect();
}

export interface ImageFacts {
  readonly postgres: { readonly image: string; readonly id: string };
  readonly mongo: { readonly image: string; readonly id: string };
}

export function collectImages(): ImageFacts {
  return {
    postgres: { image: PG_IMAGE, id: ensureImage(PG_IMAGE) },
    mongo: { image: MONGO_IMAGE, id: ensureImage(MONGO_IMAGE) },
  };
}

/** Fingerprint of everything that makes two measurements comparable. The CPU and memory identify the machine, the rest the setup. */
export function machineFingerprint(machine: MachineFacts, images: ImageFacts, plan: Plan): string {
  return fingerprintOf({
    harness: HARNESS_VERSION,
    machine: {
      cpu: machine.hostCpu,
      cores: machine.hostCores,
      memGiB: machine.hostMemGiB,
      os: machine.os,
      docker: machine.docker,
      nodeMajor: machine.node.split('.')[0],
    },
    images,
    pgSettings: PG_SETTINGS,
    plan: planDescriptor(plan),
  });
}
