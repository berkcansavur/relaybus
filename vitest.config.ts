import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const packages = [
  'core',
  'nestjs',
  'postgres',
  'mongodb',
  'kafka',
  'rabbitmq',
  'testing',
  'eslint-plugin',
];

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

/** Integration files whose timing or server-wide effects must not overlap with other files. */
const serialIntegrationTests = [
  'packages/postgres/test/lane-leases.integration.test.ts',
  'packages/postgres/test/claim-concurrency.integration.test.ts',
  // Bounded shutdown: asserts how long stop takes on the wall clock.
  'packages/postgres/test/stop-deadline.integration.test.ts',
  // Ten dispatchers with their own pools or clients for about 20 s each.
  'packages/postgres/test/stress.integration.test.ts',
  'packages/mongodb/test/stress.integration.test.ts',
  'packages/mongodb/test/mongo-lane-leases.integration.test.ts',
  'packages/rabbitmq/test/broker-restart.integration.test.ts',
  // End-to-end flows across a broker with wall-clock waits: under the parallel load of the
  // other files they missed their deadlines now and then.
  'packages/kafka/test/interoperability.integration.test.ts',
  'packages/rabbitmq/test/interoperability.integration.test.ts',
  'packages/kafka/test/codecs.integration.test.ts',
  'packages/rabbitmq/test/codecs.integration.test.ts',
  'packages/kafka/test/catalog-services.integration.test.ts',
  'packages/rabbitmq/test/catalog-services.integration.test.ts',
  'packages/nestjs/test/catalog-services.integration.test.ts',
  // Transaction budgets of a few hundred milliseconds against wall-clock sleeps.
  'packages/postgres/test/transaction-timeout.integration.test.ts',
  'packages/mongodb/test/transaction-timeout.integration.test.ts',
];

const integration = {
  environment: 'node' as const,
  testTimeout: 120_000,
  hookTimeout: 180_000,
  globalSetup: ['./vitest.global-setup.ts'],
};

export default defineConfig({
  resolve: { alias },
  test: {
    projects: [
      {
        resolve: { alias },
        test: {
          name: 'unit',
          include: [
            'packages/*/test/**/*.test.ts',
            'benchmarks/test/**/*.test.ts',
            'eslint-rules/**/*.test.mjs',
          ],
          exclude: ['packages/*/test/**/*.integration.test.ts'],
          environment: 'node',
        },
      },
      {
        resolve: { alias },
        test: {
          ...integration,
          name: 'integration',
          include: ['packages/*/test/**/*.integration.test.ts'],
          exclude: ['**/node_modules/**', ...serialIntegrationTests],
          // Files share one server per backend and keep apart through unique names.
          fileParallelism: true,
          sequence: { groupOrder: 0 },
        },
      },
      {
        resolve: { alias },
        test: {
          ...integration,
          name: 'integration-serial',
          include: serialIntegrationTests,
          // Lease timing and broker restarts: one file at a time, after everything else.
          fileParallelism: false,
          sequence: { groupOrder: 1 },
        },
      },
    ],
    coverage: {
      provider: 'v8',
      include: ['packages/*/src/**/*.ts'],
      exclude: ['packages/*/src/**/index.ts', 'packages/core/src/standard-schema.ts'],
      reporter: ['text', 'html', 'lcov', 'json-summary'],
      thresholds: {
        statements: 100,
        branches: 100,
        functions: 100,
        lines: 100,
      },
    },
  },
});
