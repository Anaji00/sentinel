import { defineConfig } from 'vitest/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * The `@/` alias, for the test runner.
 *
 * `tsconfig.json` maps `@/*` to `./src/*` and Next resolves it at build time,
 * so thirty files across `app/` import that way and always have. Vitest ran
 * without a config and therefore without the alias -- which went unnoticed only
 * because the tests happened to import relative paths. The first test to reach
 * a component that imports `@/components/ui/icons` failed at module resolution,
 * not at an assertion.
 *
 * The file is `.mts` because vitest's native config loader reads a `.ts`
 * config as CommonJS and warns about the ESM syntax in it.
 *
 * One mapping, matching tsconfig exactly. Everything else stays on vitest's
 * defaults so adding this file does not quietly change what gets collected.
 */
export default defineConfig({
  resolve: {
    alias: {
      '@': path.resolve(fileURLToPath(new URL('.', import.meta.url)), 'src'),
    },
  },
});
