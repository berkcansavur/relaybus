#!/usr/bin/env node
// Checks that installing @relaybus/postgres into an application that already uses Prisma leaves
// that application's Prisma alone: no second @prisma/client at the root, the version unchanged,
// and `prisma generate` still working. 0.4.0 failed this on Prisma 5 (its optional peer range let
// npm install Prisma Client 7 at the root).
//
// Usage: node scripts/check-prisma-install.mjs [prisma version, default 5.22.0] [--package <spec>]
//   --package installs <spec> (for example @relaybus/postgres@0.4.0) instead of tarballs packed
//   from this checkout, to show what a published version does.
import { execFileSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const args = process.argv.slice(2);
const packageIndex = args.indexOf('--package');
const publishedSpec = packageIndex === -1 ? undefined : args[packageIndex + 1];
const prismaVersion =
  args.find(
    (arg, index) => !arg.startsWith('--') && (packageIndex === -1 || index !== packageIndex + 1),
  ) ?? '5.22.0';
const root = resolve(import.meta.dirname, '..');
const work = mkdtempSync(join(tmpdir(), 'relaybus-prisma-install-'));
const app = join(work, 'app');

function run(command, commandArgs, cwd) {
  return execFileSync(command, commandArgs, {
    cwd,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, CHECKPOINT_DISABLE: '1', PRISMA_HIDE_UPDATE_MESSAGE: '1' },
  });
}

/** Tarballs of @relaybus/core (the peer of postgres) and @relaybus/postgres packed from this checkout. */
function packTarballs() {
  const tarballs = join(work, 'tarballs');
  for (const name of ['core', 'postgres']) {
    run('pnpm', ['pack', '--pack-destination', tarballs], join(root, 'packages', name));
  }
  return readdirSync(tarballs).map((file) => join(tarballs, file));
}

/** Every @prisma/client directory under node_modules, with its version. */
function prismaClients(directory, found = []) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }
    const path = join(directory, entry.name);
    if (path.endsWith(join('@prisma', 'client')) && existsSync(join(path, 'package.json'))) {
      found.push(
        `${path.slice(app.length + 1)}@${JSON.parse(readFileSync(join(path, 'package.json'), 'utf8')).version}`,
      );
    }
    if (entry.name !== '.prisma') {
      prismaClients(path, found);
    }
  }
  return found;
}

const npmInstall = ['install', '--no-audit', '--no-fund', '--loglevel=error'];
mkdirSync(app);
run('npm', ['init', '-y'], app);
run('npm', [...npmInstall, `@prisma/client@${prismaVersion}`, `prisma@${prismaVersion}`], app);
writeFileSync(
  join(app, 'schema.prisma'),
  'generator client {\n  provider = "prisma-client-js"\n}\n\ndatasource db {\n  provider = "postgresql"\n  url      = env("DATABASE_URL")\n}\n\nmodel Order {\n  id String @id\n}\n',
);
/** Runs a step; a failure becomes its first error line instead of an exception. */
function attempt(step) {
  try {
    step();
    return 'ok';
  } catch (error) {
    const lines = String(error.stderr ?? error.message)
      .trim()
      .split('\n');
    return `failed: ${lines.find((line) => /error|Error/.test(line)) ?? lines.at(-1)}`;
  }
}

const before = prismaClients(join(app, 'node_modules'));
const install = attempt(() =>
  run(
    'npm',
    [...npmInstall, ...(publishedSpec === undefined ? packTarballs() : [publishedSpec])],
    app,
  ),
);
const after = prismaClients(join(app, 'node_modules'));
const generate = attempt(() =>
  run('npx', ['prisma', 'generate', '--schema', 'schema.prisma'], app),
);
const installed = publishedSpec ?? 'this checkout';
console.log(`Prisma ${prismaVersion} app, installed ${installed} (in ${work})`);
console.log(`npm install:           ${install}`);
console.log(`@prisma/client before: ${before.join(', ')}`);
console.log(`@prisma/client after:  ${after.join(', ')}`);
console.log(`prisma generate:       ${generate}`);
const safe =
  install === 'ok' && JSON.stringify(before) === JSON.stringify(after) && generate === 'ok';
console.log(
  safe
    ? 'PASS: the application keeps its Prisma'
    : "FAIL: the install failed or changed the application's Prisma",
);
process.exit(safe ? 0 : 1);
