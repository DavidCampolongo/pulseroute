import js from "@eslint/js";
import { defineConfig, globalIgnores } from "eslint/config";
import eslintConfigPrettier from "eslint-config-prettier/flat";
import tseslint from "typescript-eslint";

export default defineConfig(
  globalIgnores(["**/dist/**", "**/coverage/**", "**/.next/**"]),

  {
    files: ["**/*.{js,mjs,cjs,jsx}"],
    extends: [js.configs.recommended],
  },

  {
    files: ["**/*.{ts,mts,cts,tsx}"],
    extends: [js.configs.recommended, tseslint.configs.recommended],
    rules: {
      "no-undef": "off",
    },
  },

  eslintConfigPrettier,
);
