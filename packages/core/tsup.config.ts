import { defineConfig } from 'tsup';
import { packageBuild } from '../../tsup.base';

// `internal/index` is the `@relaybus/core/internal` subpath used by the other relaybus packages.
// Splitting (also for CommonJS) keeps one copy of every module, so both entries throw the same
// error classes and `instanceof` holds between them.
export default defineConfig({
  ...packageBuild,
  entry: { index: 'src/index.ts', 'internal/index': 'src/internal/index.ts' },
  splitting: true,
});
