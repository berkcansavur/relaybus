import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

// Used only by Stryker (see stryker.config.json). Mutation testing runs the
// fast, Docker-free unit project; integration tests are excluded on purpose.
const packages = ['core', 'nestjs', 'postgres', 'mongodb', 'kafka', 'rabbitmq', 'testing'];

// Stryker runs in place (`inPlace: true`), so these paths are the mutated files.
// Without aliases `@relaybus/*` would resolve through the pnpm workspace symlinks
// to the packages' built `dist/` output, which holds no mutants.
const alias = Object.fromEntries(
  packages.map((name) => [
    `@relaybus/${name}`,
    fileURLToPath(new URL(`./packages/${name}/src/index.ts`, import.meta.url)),
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
