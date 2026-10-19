import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { repoRoot } from './deps.ts';
import { PG_SETTINGS } from './pg-backend.ts';

const sh = (command: string, args: string[]): string => {
  try {
    return execFileSync(command, args, { encoding: 'utf8', cwd: repoRoot }).trim();
  } catch {
    return 'unavailable';
  }
};

export function collectEnvironment(): Record<string, unknown> {
  const packages: Record<string, string> = {};
  for (const name of ['core', 'postgres', 'mongodb']) {
    packages[`@relaybus/${name}`] = JSON.parse(readFileSync(resolve(repoRoot, 'packages', name, 'package.json'), 'utf8')).version;
  }
  const driver = (pkg: string, from: string): string =>
    JSON.parse(readFileSync(resolve(repoRoot, 'packages', from, 'node_modules', pkg, 'package.json'), 'utf8')).version;
  const dockerInfo = sh('docker', ['info', '--format', '{{.NCPU}} CPUs, {{.MemTotal}} bytes, {{.OperatingSystem}}, server {{.ServerVersion}}']);
  return {
    date: new Date().toISOString(),
    commit: sh('git', ['rev-parse', 'HEAD']),
    commitSubject: sh('git', ['log', '-1', '--format=%s']),
    describe: sh('git', ['describe', '--tags', '--always']),
    dirtyPackages: sh('git', ['status', '--porcelain', '--', 'packages']) || 'clean',
    relaybusPackages: packages,
    drivers: { pg: driver('pg', 'postgres'), mongodb: driver('mongodb', 'mongodb') },
    node: process.version,
    hostCpu: sh('sysctl', ['-n', 'machdep.cpu.brand_string']),
    hostCores: sh('sysctl', ['-n', 'hw.ncpu']),
    hostPerfCores: sh('sysctl', ['-n', 'hw.perflevel0.logicalcpu']),
    hostMemGiB: Math.round(Number(sh('sysctl', ['-n', 'hw.memsize'])) / 2 ** 30),
    macOS: sh('sw_vers', ['-productVersion']),
    docker: dockerInfo,
    containerLimits: 'none (no --cpus / --memory); the Docker Desktop VM is the limit',
    pgSettings: PG_SETTINGS,
    harness: 'Node workers run on the macOS host; databases run in Docker Desktop (VM). Both share the same physical cores.',
  };
}
