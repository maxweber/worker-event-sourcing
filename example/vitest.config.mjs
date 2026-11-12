// The Object itself, tested inside workerd: `npm test` builds the bundle
// wrangler.jsonc names and runs test/worker/ against it.
import { defineConfig } from 'vitest/config';
import { cloudflareTest } from '@cloudflare/vitest-plugin';

export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.jsonc' } })],
  test: {
    include: ['test/worker/**/*.test.mjs'],
    // An error thrown inside a Durable Object reaches its caller as the
    // rejection of the call, which the test asserts on; workerd reports
    // it as uncaught in the Object as well, which is not a test failure.
    onUnhandledError(error) {
      if (error.durableObjectId) return false;
    },
  },
});
