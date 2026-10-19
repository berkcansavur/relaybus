// Resolves relaybus (built dist, via the workspace) and the drivers/testcontainers
// from the packages that already depend on them, so the benchmarks need no install step.
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

export const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const pgRequire = createRequire(resolve(repoRoot, 'packages/postgres/package.json'));
const mongoRequire = createRequire(resolve(repoRoot, 'packages/mongodb/package.json'));
const dist = (name: string) => pathToFileURL(resolve(repoRoot, 'packages', name, 'dist/index.js')).href;

export const pg = pgRequire('pg');
export const mongodb = mongoRequire('mongodb');
export const core = await import(dist('core'));
export const postgresPkg = await import(dist('postgres'));
export const mongoPkg = await import(dist('mongodb'));

export function loadContainers() {
  return {
    PostgreSqlContainer: pgRequire('@testcontainers/postgresql').PostgreSqlContainer,
    MongoDBContainer: mongoRequire('@testcontainers/mongodb').MongoDBContainer,
  };
}
