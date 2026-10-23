// A side of a comparison is a git ref built into its own directory (git archive + install + build), cached by commit
// SHA, so a ref is built once and then reused by every later run.
import { execFileSync, spawn } from 'node:child_process';
import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export interface PreparedSide {
  readonly ref: string;
  readonly sha: string;
  /** Directory holding the built checkout (`packages/*\/dist`, installed `node_modules`). */
  readonly root: string;
}

const READY_MARKER = '.bench-ready';
const BUILD_FILTERS = ['--filter', '@relaybus/postgres...', '--filter', '@relaybus/mongodb...'];

const git = (repository: string, args: readonly string[]): string =>
  execFileSync('git', ['-C', repository, ...args], { encoding: 'utf8' }).trim();

/** Full commit SHA of a ref. */
export function resolveSha(repository: string, ref: string): string {
  return git(repository, ['rev-parse', '--verify', `${ref}^{commit}`]);
}

/** True when `packages/` has uncommitted changes, which a ref-based build does not see. */
export function packagesAreDirty(repository: string): boolean {
  return git(repository, ['status', '--porcelain', '--', 'packages']) !== '';
}

function run(
  command: string,
  args: readonly string[],
  cwd: string,
  log: (line: string) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env: { ...process.env, HUSKY: '0', CI: '1' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let tail = '';
    const keep = (chunk: Buffer): void => {
      tail = (tail + chunk.toString()).slice(-2_000);
    };
    child.stdout.on('data', keep);
    child.stderr.on('data', keep);
    child.on('error', reject);
    child.on('exit', (code) => {
      if (code === 0) resolve();
      else {
        log(tail);
        reject(new Error(`${command} ${args.join(' ')} exited with code ${code ?? 'null'}`));
      }
    });
  });
}

/** Builds `ref` of `repository` into `<cacheRoot>/<sha>` unless that directory is already built. */
export async function prepareSide(
  repository: string,
  ref: string,
  cacheRoot: string,
  log: (line: string) => void,
): Promise<PreparedSide> {
  const sha = resolveSha(repository, ref);
  const root = join(cacheRoot, sha);
  if (existsSync(join(root, READY_MARKER))) {
    log(`[side] ${ref} (${sha.slice(0, 8)}) already built`);
    return { ref, sha, root };
  }
  log(`[side] building ${ref} (${sha.slice(0, 8)}) into ${root}`);
  rmSync(root, { recursive: true, force: true });
  mkdirSync(root, { recursive: true });
  await new Promise<void>((resolve, reject) => {
    const archive = spawn('git', ['-C', repository, 'archive', sha], {
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    const extract = spawn('tar', ['-x', '-C', root], { stdio: ['pipe', 'inherit', 'inherit'] });
    archive.stdout.pipe(extract.stdin);
    archive.on('error', reject);
    extract.on('error', reject);
    extract.on('exit', (code) =>
      code === 0 ? resolve() : reject(new Error(`tar exited with code ${code ?? 'null'}`)),
    );
  });
  await run('pnpm', ['install', '--frozen-lockfile', '--prefer-offline'], root, log);
  await run('pnpm', [...BUILD_FILTERS, 'run', 'build'], root, log);
  for (const name of ['core', 'postgres', 'mongodb']) {
    if (!existsSync(join(root, 'packages', name, 'dist/index.js')))
      throw new Error(`[side] ${name} has no dist/index.js after the build of ${ref}`);
  }
  writeFileSync(join(root, READY_MARKER), `${sha}\n`);
  return { ref, sha, root };
}
