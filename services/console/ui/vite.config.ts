import { fileURLToPath } from 'node:url';
import react from '@vitejs/plugin-react';
import { searchForWorkspaceRoot } from 'vite';
import { defineConfig } from 'vitest/config';

// The shared dual-theme source (tokens CSS + the framework-free theme script) lives at the repo
// root in web/theme so Docs, Homepage and Gatus can use the same files. The Docker ui stage copies
// it to the same relative location.
const THEME_DIR = fileURLToPath(new URL('../../../web/theme', import.meta.url));

// The React console is the console UI, served at /console/. Vite content-hashes everything
// under assets/, which the Go AssetRegistry serves immutable.
export default defineConfig({
  base: '/console/',
  plugins: [react()],
  resolve: {
    alias: { '@aerial-theme': THEME_DIR },
  },
  server: {
    fs: { allow: [searchForWorkspaceRoot(process.cwd()), THEME_DIR] },
  },
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
    include: ['src/**/*.test.{ts,tsx}'],
    restoreMocks: true,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.{ts,tsx}'],
      exclude: ['src/main.tsx', 'src/**/*.test.{ts,tsx}', 'src/test/**'],
      thresholds: { lines: 95, branches: 95, functions: 95, statements: 95 },
    },
  },
});
