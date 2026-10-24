// Loads one side of a comparison: the built `dist/` of core, postgres and mongodb of a checkout, together with the
// drivers that checkout resolves. Loading two checkouts in one process gives two independent module graphs.
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import type * as Core from '@relaybus/core';
import type * as PostgresPackage from '@relaybus/postgres';
import type * as MongoPackage from '@relaybus/mongodb';
import type * as Pg from 'pg';
import type * as MongoDriver from 'mongodb';
import type * as PostgresContainers from '@testcontainers/postgresql';
import type * as MongoContainers from '@testcontainers/mongodb';

export interface SideModules {
  readonly root: string;
  readonly core: typeof Core;
  readonly postgres: typeof PostgresPackage;
  readonly mongoPackage: typeof MongoPackage;
  readonly pg: typeof Pg;
  readonly mongoDriver: typeof MongoDriver;
}

const distUrl = (root: string, name: string): string =>
  pathToFileURL(resolve(root, 'packages', name, 'dist/index.js')).href;

/** Imports the built packages of the checkout at `root` (run `pnpm install` and the build there first). */
export async function loadSide(root: string): Promise<SideModules> {
  const postgresRequire = createRequire(resolve(root, 'packages/postgres/package.json'));
  const mongoRequire = createRequire(resolve(root, 'packages/mongodb/package.json'));
  return {
    root,
    core: (await import(distUrl(root, 'core'))) as typeof Core,
    postgres: (await import(distUrl(root, 'postgres'))) as typeof PostgresPackage,
    mongoPackage: (await import(distUrl(root, 'mongodb'))) as typeof MongoPackage,
    pg: postgresRequire('pg') as typeof Pg,
    mongoDriver: mongoRequire('mongodb') as typeof MongoDriver,
  };
}

export interface Containers {
  readonly PostgreSqlContainer: typeof PostgresContainers.PostgreSqlContainer;
  readonly MongoDBContainer: typeof MongoContainers.MongoDBContainer;
}

/** Testcontainers come from the harness checkout (the one running the benchmark), not from a measured side. */
export function loadContainers(harnessRoot: string): Containers {
  const postgresRequire = createRequire(resolve(harnessRoot, 'packages/postgres/package.json'));
  const mongoRequire = createRequire(resolve(harnessRoot, 'packages/mongodb/package.json'));
  return {
    PostgreSqlContainer: (
      postgresRequire('@testcontainers/postgresql') as typeof PostgresContainers
    ).PostgreSqlContainer,
    MongoDBContainer: (mongoRequire('@testcontainers/mongodb') as typeof MongoContainers)
      .MongoDBContainer,
  };
}

export const harnessRoot = resolve(import.meta.dirname, '..', '..');
