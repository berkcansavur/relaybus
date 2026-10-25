import type { ESLint, Linter, Rule } from 'eslint';
import { noExternalCallInTransaction } from './rules/no-external-call-in-transaction.js';
import { publishRequiresTransaction } from './rules/publish-requires-transaction.js';

export const rules: Readonly<Record<string, Rule.RuleModule>> = {
  'no-external-call-in-transaction': noExternalCallInTransaction,
  'publish-requires-transaction': publishRequiresTransaction,
};

const plugin = { meta: { name: '@relaybus/eslint-plugin' }, rules } satisfies ESLint.Plugin;

/**
 * Both rules at error level, under the plugin name `@relaybus`:
 *
 * ```js
 * // eslint.config.js
 * import relaybus from '@relaybus/eslint-plugin';
 * export default [relaybus.configs.recommended];
 * ```
 */
export const configs: { readonly recommended: Linter.Config } = {
  recommended: {
    name: '@relaybus/recommended',
    plugins: { '@relaybus': plugin },
    rules: {
      '@relaybus/no-external-call-in-transaction': 'error',
      '@relaybus/publish-requires-transaction': 'error',
    },
  },
};

const relaybus: ESLint.Plugin & { readonly configs: typeof configs } = { ...plugin, configs };

export default relaybus;
