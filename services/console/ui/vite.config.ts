import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

// The React console is served side by side with the vanilla UI under /console/next/.
// Vite content-hashes everything under assets/, which the Go AssetRegistry serves immutable.
export default defineConfig({
  base: '/console/next/',
  plugins: [react()],
  build: {
    target: 'es2023',
    outDir: 'dist',
    assetsDir: 'assets',
    emptyOutDir: true,
    sourcemap: false,
    modulePreload: { polyfill: false },
  },
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.{ts,tsx}', 'e2e/**/*.test.ts'],
    restoreMocks: true,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.{ts,tsx}'],
      exclude: ['src/main.tsx', 'src/**/*.test.{ts,tsx}'],
      thresholds: { lines: 95, branches: 95, functions: 95, statements: 95 },
    },
  },
});
