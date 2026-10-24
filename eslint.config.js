import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import logPrefix from './eslint-rules/log-prefix.js';

export default tseslint.config(
  {
    ignores: [
      '**/dist/**',
      '**/coverage/**',
      'reports/**',
      '.stryker-tmp/**',
      'benchmarks/.cache/**',
      'docs-site/.vitepress/cache/**',
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
    // Owner's logging rule: a log line names the class and method it is written in.
    files: ['packages/*/src/**/*.ts'],
    plugins: { relaybus: { rules: { 'log-prefix': logPrefix } } },
    rules: { 'relaybus/log-prefix': 'error' },
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
    // Documentation site configuration: VitePress runs it on Node, outside the TypeScript project.
    files: ['docs-site/.vitepress/config.ts'],
    ...tseslint.configs.disableTypeChecked,
    languageOptions: {
      ...tseslint.configs.disableTypeChecked.languageOptions,
      globals: { process: 'readonly', console: 'readonly' },
    },
  },
);
