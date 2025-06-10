import js from "@eslint/js";
import globals from 'globals';
import { defineConfig } from "eslint/config";

import n from "eslint-plugin-n";
import preferArrowFunctions from "eslint-plugin-prefer-arrow";

export default defineConfig([
  {
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      globals: {
        ...globals.node,
        ...global.commonjs,
        Atomics: "readonly",
        SharedArrayBuffer: "readonly",
        _logger: "readonly",
      },
    },
    files: ["**/*.{js,mjs,cjs}"],
    plugins: {
      js,
      n,
      "prefer-arrow": preferArrowFunctions,
    },
    extends: [
      js.configs.recommended,
    ],
    rules: {
      semi: [2, "always"],
      "no-warning-comments": [
        0,
        { terms: ["todo", "fixme", "xxx", "debug"], location: "start" },
      ],
      "prefer-arrow/prefer-arrow-functions": [
        2,
        { singleReturnOnly: true, disallowPrototype: true },
      ],
      "object-curly-newline": ["error", { multiline: true }],
      "arrow-parens": [2, "as-needed"],
      "arrow-body-style": [2, "as-needed"],
      "operator-linebreak": [2, "after"],
      indent: [
        "error",
        2,
        { ignoredNodes: ["TemplateLiteral > *"], SwitchCase: 1 },
      ],
      "no-unused-expressions": 0,
    },
  },
]);
