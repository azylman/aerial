import { defineConfig } from '@playwright/test';
import { BASE_URL } from './e2e/constants.ts';

// Browser e2e suite: the React console at /console/ served by the real console binary
// (e2e/server.ts). Run `npm run build` first; CI runs it in the Playwright image.
export default defineConfig({
  testDir: './e2e',
  testMatch: '*.spec.ts',
  outputDir: './e2e/artifacts/test-results',
  fullyParallel: false,
  workers: 1,
  retries: 0,
  forbidOnly: process.env['CI'] !== undefined,
  reporter: [['list']],
  use: { baseURL: BASE_URL, browserName: 'chromium' },
  webServer: {
    command: 'node e2e/server.ts',
    url: `${BASE_URL}/health`,
    reuseExistingServer: false,
    timeout: 120_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
});
