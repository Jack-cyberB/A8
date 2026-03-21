import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: false,
  workers: 1,
  timeout: 60000,
  expect: {
    timeout: 15000,
  },
  use: {
    baseURL: 'http://127.0.0.1:8010',
    headless: true,
    viewport: { width: 1600, height: 900 },
    actionTimeout: 15000,
    trace: 'retain-on-failure',
  },
  webServer: {
    command: 'python -c "from backend.server import run; run(port=8010)"',
    url: 'http://127.0.0.1:8010',
    reuseExistingServer: false,
    timeout: 120000,
  },
});
