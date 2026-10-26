import { defineConfig } from 'tsup';
import { packageBuild } from '../../tsup.base';

export default defineConfig({
  ...packageBuild,
  entry: {
    index: 'src/index.ts',
    'codec-contract': 'src/codec-contract/index.ts',
    jest: 'src/jest.ts',
  },
  external: ['vitest', '@relaybus/core'],
  // One shared chunk in both formats, so every entry point exports the same CodecContractError
  // class (`instanceof` holds across `/jest`, `/codec-contract` and the main entry).
  splitting: true,
});
