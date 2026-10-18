#!/usr/bin/env node
// Packs and publishes every packages/* workspace with one shared version.
// Usage: node scripts/publish-packages.mjs <version> [dist-tag] [--dry-run]
//
// - `pnpm pack` rewrites workspace:* / workspace:^ ranges to the real version.
// - `npm publish <tarball>` authenticates with npm Trusted Publishing (OIDC).
// - A package whose exact version is already on npm is skipped, so a partially
//   failed release can simply be re-run.
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readdirSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const args = process.argv.slice(2).filter((arg) => !arg.startsWith('--'));
const dryRun = process.argv.includes('--dry-run');
const version = args[0];
const rawTag = args[1];
const distTag = !rawTag || ['undefined', 'false', 'null'].includes(rawTag) ? 'latest' : rawTag;

if (!version) {
  console.error('Usage: node scripts/publish-packages.mjs <version> [dist-tag] [--dry-run]');
  process.exit(1);
}

// core and testing first (every other package depends on them), then the rest.
const FIRST = ['core', 'testing'];
const packagesDir = resolve(import.meta.dirname, '..', 'packages');
const dirs = readdirSync(packagesDir, { withFileTypes: true })
  .filter(
    (entry) => entry.isDirectory() && existsSync(join(packagesDir, entry.name, 'package.json')),
  )
  .map((entry) => entry.name);
const ordered = [
  ...FIRST.filter((name) => dirs.includes(name)),
  ...dirs.filter((name) => !FIRST.includes(name)).sort(),
];

function run(command, commandArgs, options = {}) {
  return execFileSync(command, commandArgs, {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    ...options,
  });
}

function existsOnNpm(name, wanted) {
  try {
    const out = run('npm', ['view', `${name}@${wanted}`, 'version', '--json']).trim();
    return out.length > 0 && JSON.parse(out) === wanted;
  } catch (error) {
    const output = `${error.stdout ?? ''}${error.stderr ?? ''}`;
    if (output.includes('E404')) return false;
    throw error;
  }
}

const packDir = mkdtempSync(join(tmpdir(), 'relaybus-pack-'));
const published = [];
const skipped = [];

for (const dir of ordered) {
  const cwd = join(packagesDir, dir);
  const manifest = JSON.parse(readFileSync(join(cwd, 'package.json'), 'utf8'));
  if (manifest.private) continue;
  if (manifest.version !== version) {
    console.error(
      `[publish-packages] ${manifest.name} is at ${manifest.version}, expected ${version}. Run scripts/set-version.mjs first.`,
    );
    process.exit(1);
  }
  if (existsOnNpm(manifest.name, version)) {
    console.log(`[publish-packages] ${manifest.name}@${version} already on npm, skipping`);
    skipped.push(manifest.name);
    continue;
  }
  console.log(`[publish-packages] packing ${manifest.name}@${version}`);
  run('pnpm', ['pack', '--pack-destination', packDir], { cwd });
  const tarball = join(
    packDir,
    `${manifest.name.replace('@', '').replace('/', '-')}-${version}.tgz`,
  );
  if (!existsSync(tarball)) {
    console.error(`[publish-packages] Expected tarball not found: ${tarball}`);
    process.exit(1);
  }
  const publishArgs = ['publish', tarball, '--access', 'public', '--tag', distTag];
  // Provenance needs the CI OIDC identity, so a local dry run leaves it out.
  if (dryRun) publishArgs.push('--dry-run');
  else publishArgs.push('--provenance');
  console.log(`[publish-packages] npm ${publishArgs.join(' ')}`);
  console.log(run('npm', publishArgs, { cwd }));
  published.push(manifest.name);
}

console.log(
  `[publish-packages] done${dryRun ? ' (dry run)' : ''}: published ${published.length}, skipped ${skipped.length}, tag ${distTag}`,
);
