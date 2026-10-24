import { RuleTester } from 'eslint';
import tseslint from 'typescript-eslint';
import { afterAll, describe, it } from 'vitest';
import logPrefix from './log-prefix.js';

RuleTester.afterAll = afterAll;
RuleTester.describe = describe;
RuleTester.it = it;

const tester = new RuleTester({ languageOptions: { parser: tseslint.parser } });

tester.run('log-prefix', logPrefix, {
  valid: [
    'class Store { #claim() { this.#logger.warn(`[Store](claim) Claim failed | owner=${owner}`); } }',
    "class Store { constructor() { logger.error('[Store](constructor) Pool failed'); } }",
    // A closure belongs to the method that contains it.
    'class Store { start() { pool.on("error", (e) => this.#settings.logger.error(`[Store](start) Failed | error=${e}`)); } }',
    // Outside a class: the module main class and the enclosing function.
    "export class Store {}\nfunction connect() { logger.error('[Store](connect) Idle connection failed'); }",
    "export function decode() { console.warn('[decode](decode) Skipped'); }",
    // Not a logger.
    "class Store { run() { this.#notifier.error('anything'); } }",
  ],
  invalid: [
    {
      code: "class Claims { #claimMore() { this.#logger.error('[Store](claim) Taking more lanes failed'); } }",
      errors: [{ messageId: 'mismatch' }],
    },
    {
      code: "class Store { claim() { logger.warn('Taking more lanes failed'); } }",
      errors: [{ messageId: 'missing' }],
    },
    {
      code: 'class Transport { #withRetries(method) { this.#logger.warn(`[Transport](${method}) Attempt failed`); } }',
      errors: [{ messageId: 'missing' }],
    },
    {
      code: 'class Store { claim(message) { this.#logger.warn(message); } }',
      errors: [{ messageId: 'missing' }],
    },
    {
      code: "export class Store {}\nfunction connect() { logger.error('[Store](constructor) Idle connection failed'); }",
      errors: [{ messageId: 'mismatch' }],
    },
  ],
});
