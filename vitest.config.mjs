import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // Engine open/close and shutdown deadlines are the slow paths; unit-fast
    // assertions do not need the default 5s ceiling to bind them.
    testTimeout: 30_000,
  },
})
