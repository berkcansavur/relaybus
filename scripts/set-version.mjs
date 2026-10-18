#!/usr/bin/env node
// Writes one shared version into every packages/*/package.json.
// Usage: node scripts/set-version.mjs <version>
// Only the top-level "version" line is replaced so formatting stays intact.
import { readdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { join, resolve } from 'node:path';

const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$/;

const version = process.argv[2];
if (!version || !SEMVER.test(version)) {
  console.error('Usage: node scripts/set-version.mjs <semver>');
  process.exit(1);
}

const packagesDir = resolve(import.meta.dirname, '..', 'packages');
let updated = 0;
for (const entry of readdirSync(packagesDir, { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const file = join(packagesDir, entry.name, 'package.json');
  if (!existsSync(file)) continue;
  const source = readFileSync(file, 'utf8');
  const next = source.replace(/^(\s{2}"version":\s*)"[^"]*"/m, `$1"${version}"`);
  if (next === source && !source.includes(`"version": "${version}"`)) {
    console.error(`[set-version] No top-level "version" field found in ${file}`);
    process.exit(1);
  }
  writeFileSync(file, next);
  updated += 1;
  console.log(`[set-version] ${entry.name} -> ${version}`);
}
if (updated === 0) {
  console.error('[set-version] No packages found');
  process.exit(1);
}
