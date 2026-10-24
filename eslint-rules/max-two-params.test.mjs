import { RuleTester } from 'eslint';
import tseslint from 'typescript-eslint';
import { afterAll, describe, it } from 'vitest';
import maxTwoParams from './max-two-params.js';

RuleTester.afterAll = afterAll;
RuleTester.describe = describe;
RuleTester.it = it;

const tester = new RuleTester({ languageOptions: { parser: tseslint.parser } });

tester.run('max-two-params', maxTwoParams, {
  valid: [
    'function claim(request, { now, owner }) {}',
    'const sort = (left, right) => left - right;',
    'class Store { constructor(pool, schema, logger) {} #claim(request, input) {} }',
  ],
  invalid: [
    { code: 'function claim(request, now, owner) {}', errors: [{ messageId: 'tooMany' }] },
    { code: 'const pick = (a, b, c) => a;', errors: [{ messageId: 'tooMany' }] },
    { code: 'class Store { #claim(a, b, c, d) {} }', errors: [{ messageId: 'tooMany' }] },
    { code: 'const handler = function (a, b, c) {};', errors: [{ messageId: 'tooMany' }] },
  ],
});
