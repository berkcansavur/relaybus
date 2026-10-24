import { RuleTester } from 'eslint';
import tseslint from 'typescript-eslint';
import { afterAll, describe, it } from 'vitest';

// RuleTester runs its cases through these hooks; afterAll is not in its types.
Object.assign(RuleTester, { afterAll, describe, it, itOnly: it.only });

/** Parses TypeScript with decorators, as the rules see NestJS code. */
export const tester = new RuleTester({ languageOptions: { parser: tseslint.parser } });

/** The default parser, for JavaScript files without decorators. */
export const plainTester = new RuleTester();
