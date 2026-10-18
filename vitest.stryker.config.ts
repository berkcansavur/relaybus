import { resolve } from 'node:path';
import { defineConfig } from 'vitest/config';

// Used only by Stryker (see stryker.config.json). Mutation testing runs the
// fast, Docker-free unit project; integration tests are excluded on purpose.
const packages = ['core', 'nestjs', 'postgres', 'mongodb', 'kafka', 'rabbitmq', 'testing'];

// Resolve from the working directory, not import.meta.url: Stryker runs the
// tests inside a sandbox copy, and the aliases must point at the mutated files there.
const alias = Object.fromEntries(
  packages.map((name) => [
    `@relaybus/${name}`,
    resolve(process.cwd(), `packages/${name}/src/index.ts`),
  ]),
);

export default defineConfig({
  resolve: { alias },
  test: {
    name: 'mutation',
    include: ['packages/*/test/**/*.test.ts'],
    exclude: ['**/node_modules/**', 'packages/*/test/**/*.integration.test.ts'],
    environment: 'node',
  },
});
