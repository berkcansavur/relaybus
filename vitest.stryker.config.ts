import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';
import { serialIntegrationTests } from './vitest.config.js';

// Used only by Stryker (see stryker.config.json). Mutation testing runs the
// fast, Docker-free unit project; integration tests are excluded on purpose,
// except in the Postgres shard of mutation.yml: most of the Postgres store is
// SQL that only a real server can check, so with
// RELAYBUS_MUTATION_TESTS=postgres the Postgres integration tests run too.
// The serial ones stay out: they assert wall-clock timing, which the load of a
// mutation run would break without any mutant.
const packages = [
  'core',
  'nestjs',
  'postgres',
  'mongodb',
  'kafka',
  'rabbitmq',
  'testing',
  'eslint-plugin',
  'otel',
];

// Stryker runs in place (`inPlace: true`), so these paths are the mutated files.
// Without aliases `@relaybus/*` would resolve through the pnpm workspace symlinks
// to the packages' built `dist/` output, which holds no mutants.
const alias = Object.fromEntries([
  // Before `@relaybus/core`, which would otherwise match the subpath as a prefix.
  [
    '@relaybus/core/internal',
    fileURLToPath(new URL('./packages/core/src/internal/index.ts', import.meta.url)),
  ],
  ...packages.map((name) => [
    `@relaybus/${name}`,
    fileURLToPath(new URL(`./packages/${name}/src/index.ts`, import.meta.url)),
  ]),
]);

const postgres = process.env['RELAYBUS_MUTATION_TESTS'] === 'postgres';

export default defineConfig({
  resolve: { alias },
  test: {
    name: 'mutation',
    include: ['packages/*/test/**/*.test.ts'],
    exclude: [
      '**/node_modules/**',
      postgres
        ? 'packages/!(postgres)/test/**/*.integration.test.ts'
        : 'packages/*/test/**/*.integration.test.ts',
      ...(postgres ? serialIntegrationTests : []),
    ],
    environment: 'node',
    ...(postgres
      ? {
          testTimeout: 120_000,
          hookTimeout: 180_000,
          globalSetup: ['./vitest.global-setup.ts'],
        }
      : {}),
  },
});
