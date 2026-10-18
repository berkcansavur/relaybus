import type { Options } from 'tsup';

export const packageBuild: Options = {
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  sourcemap: true,
  clean: true,
  target: 'node22',
  treeshake: true,
  tsconfig: 'tsconfig.build.json',
};
