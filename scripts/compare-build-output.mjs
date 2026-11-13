#!/usr/bin/env node
// Compares the built JavaScript of every package with a version published on npm.
//
// Usage: node scripts/compare-build-output.mjs <version>
// Run `pnpm build` first. Downloads @relaybus/<name>@<version> for every package in
// packages/ and compares each .js, .cjs and .mjs file under dist/ byte for byte. Exits 1
// when a file differs, exists on one side only, or a package has no such version. Type
// declarations and source maps are not compared: the API reports cover the declarations.
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const version = process.argv[2];
if (!version) {
  console.error('Usage: node scripts/compare-build-output.mjs <version>');
  process.exit(1);
}

/** Runtime files under a dist folder, as paths relative to it. */
function runtimeFiles(distDir) {
  return readdirSync(distDir, { recursive: true })
    .map(String)
    .filter((file) => /\.(c|m)?js$/.test(file))
    .sort();
}

const workDir = mkdtempSync(join(tmpdir(), 'relaybus-build-output-'));
const problems = [];
let compared = 0;
try {
  const packageDirs = readdirSync(join(root, 'packages'), { withFileTypes: true })
    .filter((dirent) => dirent.isDirectory())
    .map((dirent) => join(root, 'packages', dirent.name));
  for (const packageDir of packageDirs) {
    const { name } = JSON.parse(readFileSync(join(packageDir, 'package.json'), 'utf8'));
    const extractDir = join(workDir, relative(join(root, 'packages'), packageDir));
    try {
      const tarball = execFileSync(
        'npm',
        ['pack', `${name}@${version}`, '--silent', '--pack-destination', workDir],
        { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
      ).trim();
      mkdirSync(extractDir, { recursive: true });
      execFileSync('tar', ['-xzf', join(workDir, tarball), '-C', extractDir]);
    } catch {
      problems.push(`${name}@${version} could not be downloaded`);
      continue;
    }
    const publishedDist = join(extractDir, 'package', 'dist');
    const builtDist = join(packageDir, 'dist');
    const published = runtimeFiles(publishedDist);
    const built = runtimeFiles(builtDist);
    for (const file of new Set([...published, ...built])) {
      if (!published.includes(file)) problems.push(`${name}: dist/${file} is new`);
      else if (!built.includes(file)) problems.push(`${name}: dist/${file} is missing`);
      else if (
        !readFileSync(join(publishedDist, file)).equals(readFileSync(join(builtDist, file)))
      ) {
        problems.push(`${name}: dist/${file} differs`);
      } else compared += 1;
    }
  }
} finally {
  rmSync(workDir, { recursive: true, force: true });
}

if (problems.length > 0) {
  console.error(
    `[compare-build-output](main) Build output differs from ${version}:\n  - ${problems.join('\n  - ')}`,
  );
  process.exit(1);
}
console.log(`[compare-build-output](main) ${compared} files identical to ${version}`);
