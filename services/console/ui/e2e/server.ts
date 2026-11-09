// Playwright webServer: runs the real console Go binary with the same static layout as the Docker
// image (static/ plus the Vite build at static/next/). Run `npm run build` first.
//
// The binary is built from services/console unless CONSOLE_E2E_BIN points at a prebuilt one (CI
// builds it on the runner, then runs the suite inside the Playwright image, which has no Go).
// Upstream URLs point at a closed port and outbound proxies at a dead address: every /api/* call
// the browser makes is answered from fixtures by page.route, so the server never needs the network.

import { type ChildProcess, execFileSync, spawn } from 'node:child_process';
import { cpSync, existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { E2E_PORT } from './constants.ts';

const UI_DIR = resolve(import.meta.dirname, '..');
const CONSOLE_DIR = resolve(UI_DIR, '..');
const DIST = join(UI_DIR, 'dist');

if (!existsSync(join(DIST, 'index.html'))) {
  console.error('[e2e] services/console/ui/dist is missing: run `npm run build` before `npm run e2e`.');
  process.exit(1);
}

const work = mkdtempSync(join(tmpdir(), 'console-e2e-'));
cpSync(join(CONSOLE_DIR, 'static'), join(work, 'static'), { recursive: true });
cpSync(DIST, join(work, 'static', 'next'), { recursive: true });

let bin = process.env['CONSOLE_E2E_BIN'] ?? '';
if (bin === '') {
  bin = join(work, 'aerial-console');
  execFileSync('go', ['build', '-o', bin, '.'], { cwd: CONSOLE_DIR, stdio: 'inherit' });
}

const DEAD = 'http://127.0.0.1:9';
const configPath = join(work, 'console.yaml');
writeFileSync(
  configPath,
  [
    `port: "${E2E_PORT}"`,
    `eri_url: ${DEAD}`,
    `memory_url: ${DEAD}`,
    `haro_url: ${DEAD}`,
    `vanadis_url: ${DEAD}`,
    `kube_api_server: ${DEAD}`,
    'github_repos:',
    '  - example/console-e2e',
    '',
  ].join('\n'),
);

const child: ChildProcess = spawn(resolve(bin), [], {
  cwd: work,
  stdio: 'inherit',
  env: {
    PATH: process.env['PATH'] ?? '',
    CONFIG_PATH: configPath,
    HTTP_PROXY: DEAD,
    HTTPS_PROXY: DEAD,
    NO_PROXY: '127.0.0.1,localhost',
  },
});

function shutdown(code: number): void {
  child.kill('SIGTERM');
  rmSync(work, { recursive: true, force: true });
  process.exit(code);
}

child.on('exit', (code) => {
  rmSync(work, { recursive: true, force: true });
  process.exit(code ?? 1);
});
process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));
