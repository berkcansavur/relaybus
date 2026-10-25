import { ESLint } from 'eslint';
import tseslint from 'typescript-eslint';
import { describe, expect, it } from 'vitest';
import relaybus, { configs, rules } from '@relaybus/eslint-plugin';

describe('@relaybus/eslint-plugin', () => {
  it('turns both rules on at error level in the recommended config', async () => {
    expect(Object.keys(rules)).toEqual([
      'no-external-call-in-transaction',
      'publish-requires-transaction',
    ]);
    expect(relaybus.configs).toBe(configs);
    const eslint = new ESLint({
      overrideConfigFile: true,
      overrideConfig: [
        { files: ['**/*.ts'], languageOptions: { parser: tseslint.parser } },
        { ...configs.recommended, files: ['**/*.ts'] },
      ],
    });
    const [result] = await eslint.lintText(
      `class S {
        @RelayBusTransaction()
        async place() { await fetch('/x'); }
        async emit() { await this.bus.publish(e); }
      }`,
      { filePath: 'service.ts' },
    );
    expect(result!.messages.map(({ ruleId, severity }) => [ruleId, severity])).toEqual([
      ['@relaybus/no-external-call-in-transaction', 2],
      ['@relaybus/publish-requires-transaction', 2],
    ]);
  });
});
