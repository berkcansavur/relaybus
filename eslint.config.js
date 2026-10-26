import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import logPrefix from './eslint-rules/log-prefix.js';
import maxTwoParams from './eslint-rules/max-two-params.js';

const ENUM_BAN = {
  selector: 'TSEnumDeclaration',
  message: 'Use a union of string literals, not an enum.',
};

/** R33: a file exports at most one class; flags the second and every later exported class. */
const ONE_EXPORTED_CLASS_PER_FILE = {
  selector:
    ':matches(ExportNamedDeclaration, ExportDefaultDeclaration):has(> ClassDeclaration) ~ ' +
    ':matches(ExportNamedDeclaration, ExportDefaultDeclaration):has(> ClassDeclaration)',
  message:
    'R33: a file exports at most one class; move the second exported class into its own file.',
};

/** Timers go through `schedule` of @relaybus/core/internal, so a clock can control them and unref stays explicit. */
const RAW_TIMER_BAN = {
  selector: 'CallExpression[callee.name=/^(setInterval|setTimeout)$/]',
  message:
    'Start timers with schedule()/sleep() from @relaybus/core/internal, which a Clock can control.',
};

const SOURCE_SYNTAX_BANS = [ENUM_BAN, ONE_EXPORTED_CLASS_PER_FILE, RAW_TIMER_BAN];

/** Code lines per source file, at most: blank and comment lines do not count. */
const MAX_FILE_LINES = { max: 300, skipBlankLines: true, skipComments: true };

/** The store contract suite: a test suite shipped as functions of @relaybus/testing. */
const CONTRACT_SUITE = [
  'packages/testing/src/event-store-contract.ts',
  'packages/testing/src/contract/**/*.ts',
];

export default tseslint.config(
  {
    ignores: [
      '**/dist/**',
      '**/coverage/**',
      'reports/**',
      '.stryker-tmp/**',
      'benchmarks/.cache/**',
      'docs-site/.vitepress/cache/**',
      'packages/postgres/test/prisma/generated/**',
    ],
  },
  js.configs.recommended,
  ...tseslint.configs.recommendedTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.json', './benchmarks/tsconfig.json'],
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-misused-promises': 'error',
      '@typescript-eslint/consistent-type-imports': ['error', { fixStyle: 'inline-type-imports' }],
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
      '@typescript-eslint/require-await': 'off',
    },
  },
  {
    // Size, complexity and style limits of the tag monorepo (coding standards R34 and the function
    // limits), as errors: the published sources have no violations left.
    files: ['packages/*/src/**/*.ts'],
    // The store contract suite is a test suite shipped as a function; tests are exempt, as in the tag monorepo.
    ignores: CONTRACT_SUITE,
    rules: {
      'max-lines': ['error', MAX_FILE_LINES],
      'max-lines-per-function': ['error', { max: 40, skipBlankLines: true, skipComments: true }],
      complexity: ['error', 10],
      'max-depth': ['error', 3],
      '@typescript-eslint/explicit-function-return-type': [
        'error',
        {
          allowExpressions: true,
          allowTypedFunctionExpressions: true,
          allowHigherOrderFunctions: true,
          allowDirectConstAssertionInArrowFunctions: true,
        },
      ],
      'no-magic-numbers': [
        'error',
        {
          ignore: [-1, 0, 1, 2],
          ignoreArrayIndexes: true,
          enforceConst: true,
          detectObjects: false,
        },
      ],
      'no-else-return': ['error', { allowElseIf: false }],
      '@typescript-eslint/no-explicit-any': 'error',
      'no-restricted-syntax': ['error', ...SOURCE_SYNTAX_BANS],
    },
  },
  {
    // The one module that starts platform timers on behalf of everything else.
    files: ['packages/core/src/internal/timers.ts'],
    rules: {
      'no-restricted-syntax': [
        'error',
        ...SOURCE_SYNTAX_BANS.filter((ban) => ban !== RAW_TIMER_BAN),
      ],
    },
  },
  {
    // Each package keeps its error classes together in errors.ts: they are one family of small classes.
    files: ['packages/*/src/errors.ts'],
    rules: {
      'no-restricted-syntax': [
        'error',
        ...SOURCE_SYNTAX_BANS.filter((ban) => ban !== ONE_EXPORTED_CLASS_PER_FILE),
      ],
    },
  },
  {
    // Owner's logging rule: a log line names the class and method it is written in.
    files: ['packages/*/src/**/*.ts'],
    plugins: { relaybus: { rules: { 'log-prefix': logPrefix, 'max-two-params': maxTwoParams } } },
    rules: { 'relaybus/log-prefix': 'error', 'relaybus/max-two-params': 'error' },
  },
  {
    // The store contract suite is a test suite shipped as a function: its test helpers keep their
    // shape and its describe blocks are long, but its files keep the file size limit.
    files: CONTRACT_SUITE,
    rules: { 'relaybus/max-two-params': 'off', 'max-lines': ['error', MAX_FILE_LINES] },
  },
  {
    files: ['packages/*/test/**/*.ts'],
    rules: {
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unsafe-argument': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/unbound-method': 'off',
      '@typescript-eslint/require-await': 'off',
    },
  },
  {
    files: [
      '*.js',
      'eslint-rules/**/*.js',
      '*.config.ts',
      'tsup.base.ts',
      'packages/*/tsup.config.ts',
    ],
    ...tseslint.configs.disableTypeChecked,
  },
  {
    // Release tooling: plain Node ESM, outside the TypeScript project.
    files: ['*.mjs', 'scripts/**/*.mjs', 'docs-site/**/*.mjs', 'eslint-rules/**/*.mjs'],
    ...tseslint.configs.disableTypeChecked,
    languageOptions: {
      ...tseslint.configs.disableTypeChecked.languageOptions,
      globals: { process: 'readonly', console: 'readonly' },
    },
  },
  {
    // pnpm install hooks: a CommonJS file pnpm loads before resolution, outside the TypeScript project.
    files: ['.pnpmfile.cjs'],
    ...tseslint.configs.disableTypeChecked,
    languageOptions: {
      ...tseslint.configs.disableTypeChecked.languageOptions,
      sourceType: 'commonjs',
      globals: { module: 'writable' },
    },
  },
  {
    // Documentation site configuration: VitePress runs it on Node, outside the TypeScript project.
    files: ['docs-site/.vitepress/config.ts'],
    ...tseslint.configs.disableTypeChecked,
    languageOptions: {
      ...tseslint.configs.disableTypeChecked.languageOptions,
      globals: { process: 'readonly', console: 'readonly' },
    },
  },
);
