import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: false,
  timeout: 60000,
  expect: {
    timeout: 15000,
  },
  use: {
    baseURL: 'http://127.0.0.1:8000',
    headless: true,
    viewport: { width: 1600, height: 900 },
    actionTimeout: 15000,
    trace: 'retain-on-failure',
  },
  webServer: {
    command: 'python backend/server.py',
    url: 'http://127.0.0.1:8000',
    reuseExistingServer: true,
    timeout: 120000,
  },
});
