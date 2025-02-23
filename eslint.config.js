import globals from 'globals';
import pluginJs from '@eslint/js';
import tsEslint from 'typescript-eslint';
import prettierPlugin from 'eslint-plugin-prettier';

export default [
  {
    files: ['**/*.{js,mjs,cjs,ts}'],
  },
  {
    languageOptions: {
      globals: globals.node,
    },
  },
  pluginJs.configs.recommended,
  ...tsEslint.configs.recommended,
  {
    // Add the Prettier plugin to run its rule:
    plugins: { prettier: prettierPlugin },
    rules: {
      'prettier/prettier': 'error',
      // Optionally, you can disable rules that conflict with Prettier here
      // (The "plugin:prettier/recommended" config normally does this.)
    },
  },
];
