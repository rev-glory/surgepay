const eslint = require('@eslint/js');
const tseslint = require('typescript-eslint');
const prettier = require('eslint-config-prettier');
const simpleImportSort = require('eslint-plugin-simple-import-sort');

module.exports = tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  prettier,
  {
    plugins: {
      'simple-import-sort': simpleImportSort,
    },
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.base.json', './packages/*/tsconfig.json'],
        tsconfigRootDir: __dirname,
      },
    },
    rules: {
      'simple-import-sort/imports': [
        'error',
        {
          groups: [
            // Node.js builtins
            [
              '^(assert|buffer|child_process|cluster|console|constants|crypto|dgram|dns|domain|events|fs|http|https|net|os|path|punycode|querystring|readline|repl|stream|string_decoder|sys|timers|tls|tty|url|util|vm|zlib|http2|dns/promises|fs/promises)($|/)',
              '^node:',
            ],
            // Packages (Third party)
            ['^@?\\w'],
            // Internal/workspace packages
            ['^(@surgepay|@common|@contracts|@events|@config|@shared)($|/)'],
            // Side effect imports
            ['^\\u0000'],
            // Parent imports, relative imports
            ['^\\.\\.(?!/?$)', '^\\.\\./?$'],
            ['^\\./(?=.*/)(?!/?$)', '^\\./+$', '^\\./?$'],
            // Other relative imports
            ['^\\.\\./.*$', '^\\./.*$'],
          ],
        },
      ],
      'simple-import-sort/exports': 'error',
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],
      'no-duplicate-imports': 'error',
      '@typescript-eslint/consistent-type-imports': [
        'error',
        {
          prefer: 'type-imports',
        },
      ],
      '@typescript-eslint/explicit-function-return-type': [
        'warn',
        {
          allowExpressions: true,
          allowTypedFunctionExpressions: true,
          allowHigherOrderFunctions: true,
          allowDirectConstAssertionInArrowFunctions: true,
        },
      ],
      '@typescript-eslint/explicit-module-boundary-types': 'error',
      'prefer-const': 'error',
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-explicit-any': 'error',
      'no-console': 'error',
    },
  },
  {
    // Exclude build artifacts and node_modules from linting
    ignores: [
      '**/dist/**',
      '**/node_modules/**',
      '**/coverage/**',
      '**/.turbo/**',
      '**/*.tsbuildinfo',
      'pnpm-lock.yaml',
      '**/jest.config.js',
      '**/generated/**',
      'packages/database/**/*.js',
      'packages/database/**/*.d.ts',
    ],
  },
  {
    // Override for bootstrap/entry points, scripts, and tests
    files: [
      '**/main.ts',
      '**/bootstrap.ts',
      'scripts/**/*.ts',
      'scripts/**/*.js',
      'eslint.config.js',
      '**/test/**/*.ts',
      '**/*.spec.ts',
      '**/*.test.ts',
    ],
    rules: {
      'no-console': 'off',
      '@typescript-eslint/explicit-module-boundary-types': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
    },
  },
  {
    // Override for messaging and serialization packages where generic envelopes require 'any'
    files: [
      'packages/common/messaging/**/*.ts',
      'packages/events/**/*.ts',
    ],
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
    },
  }
);
