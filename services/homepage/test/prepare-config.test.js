'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const os = require('os');
const prepareConfig = require('../prepare-config');
const { run, resolveDefaultCoreDir, resolveDefaultUserDir, resolveDefaultAppConfigDir, resolveYamlParser, parseYaml, safeRead, writeAtomic, copyFallback } = prepareConfig;

test('prepare-config: exports correct functions and omits startWatcher', () => {
  assert.equal(typeof run, 'function');
  assert.equal(typeof resolveDefaultCoreDir, 'function');
  assert.equal(typeof resolveDefaultUserDir, 'function');
  assert.equal(typeof resolveDefaultAppConfigDir, 'function');
  assert.equal(typeof resolveYamlParser, 'function');
  assert.equal(typeof parseYaml, 'function');
  assert.equal(typeof safeRead, 'function');
  assert.equal(typeof writeAtomic, 'function');
  assert.equal(typeof copyFallback, 'function');
  assert.equal(prepareConfig.startWatcher, undefined);
});

test('prepare-config: resolveDefaultCoreDir prefers /local/core-homepage/config when present', () => {
  const dir = resolveDefaultCoreDir((p) => p === '/local/core-homepage/config');
  assert.equal(dir, '/local/core-homepage/config');
});

test('prepare-config: resolveDefaultCoreDir falls back to /app/core-homepage/config when /local/core-homepage/config absent', () => {
  const dir = resolveDefaultCoreDir(() => false);
  assert.equal(dir, '/app/core-homepage/config');
});

test('prepare-config: resolveDefaultUserDir prefers /local/user-homepage when present', () => {
  const dir = resolveDefaultUserDir((p) => p === '/local/user-homepage');
  assert.equal(dir, '/local/user-homepage');
});

test('prepare-config: resolveDefaultUserDir falls back to /share/aerial-config/homepage when /local/user-homepage absent', () => {
  const dir = resolveDefaultUserDir(() => false);
  assert.equal(dir, '/share/aerial-config/homepage');
});

test('prepare-config: resolveDefaultAppConfigDir returns /app/config when present and null when absent', () => {
  assert.equal(resolveDefaultAppConfigDir((p) => p === '/app/config'), '/app/config');
  assert.equal(resolveDefaultAppConfigDir(() => false), null);
});

test('prepare-config: run mirrors merged custom.css and configs to appConfigDir when user custom.css is absent', () => {
  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), 'aerial-prepare-appcfg-'));
  const userDir = path.join(tmpBase, 'user');
  const coreDir = path.join(tmpBase, 'core');
  const targetDir = path.join(tmpBase, 'target');
  const appConfigDir = path.join(tmpBase, 'app-config');

  fs.mkdirSync(userDir, { recursive: true });
  fs.mkdirSync(coreDir, { recursive: true });
  fs.mkdirSync(targetDir, { recursive: true });
  fs.mkdirSync(appConfigDir, { recursive: true });

  try {
    fs.writeFileSync(path.join(coreDir, 'custom.css'), '/* core permet theme */\nbody { background: #06050a; }\n', 'utf8');
    fs.writeFileSync(path.join(coreDir, 'settings.yaml'), '{"theme":"dark","color":"slate"}\n', 'utf8');
    fs.writeFileSync(path.join(userDir, 'settings.yaml'), '{"showStats":true}\n', 'utf8');
    fs.writeFileSync(path.join(coreDir, 'extra.js'), '// core extra js\n', 'utf8');
    // Simulate upstream homepage having initialized an empty 0-byte custom.css and extra.js in /app/config
    fs.writeFileSync(path.join(appConfigDir, 'custom.css'), '', 'utf8');
    fs.writeFileSync(path.join(appConfigDir, 'extra.js'), '', 'utf8');

    const stubYaml = {
      load: (str) => JSON.parse(str),
      dump: (obj) => JSON.stringify(obj)
    };
    run({ coreDir, userDir, targetDir, appConfigDir, yaml: stubYaml, candidatePaths: [] });

    const appCss = fs.readFileSync(path.join(appConfigDir, 'custom.css'), 'utf8');
    assert.ok(appCss.includes('core permet theme'));
    const appSettings = JSON.parse(fs.readFileSync(path.join(appConfigDir, 'settings.yaml'), 'utf8'));
    assert.equal(appSettings.theme, 'dark');
    assert.equal(appSettings.showStats, true);
    assert.equal(fs.readFileSync(path.join(appConfigDir, 'extra.js'), 'utf8'), '// core extra js\n');
  } finally {
    fs.rmSync(tmpBase, { recursive: true, force: true });
  }
});

test('prepare-config: run executes clean one-shot merge', () => {
  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), 'aerial-prepare-test-'));
  const userDir = path.join(tmpBase, 'user');
  const coreDir = path.join(tmpBase, 'core');
  const targetDir = path.join(tmpBase, 'target');

  fs.mkdirSync(userDir, { recursive: true });
  fs.mkdirSync(coreDir, { recursive: true });
  fs.mkdirSync(targetDir, { recursive: true });

  try {
    fs.writeFileSync(path.join(coreDir, 'custom.css'), '/* core css */\n', 'utf8');
    fs.writeFileSync(path.join(userDir, 'custom.css'), '/* user css */\n', 'utf8');

    run({ coreDir, userDir, targetDir });

    const mergedCssPath = path.join(targetDir, 'custom.css');
    assert.ok(fs.existsSync(mergedCssPath));
    const mergedCss = fs.readFileSync(mergedCssPath, 'utf8');
    assert.ok(mergedCss.includes('core css'));
    if (resolveYamlParser()) {
      assert.ok(mergedCss.includes('user css'));
      const summaryLogPath = path.join(targetDir, 'merge-summary.log');
      assert.ok(fs.existsSync(summaryLogPath));
    }
  } finally {
    fs.rmSync(tmpBase, { recursive: true, force: true });
  }
});

test('prepare-config: candidate config fallback when /local/homepage.yaml is comment-only', () => {
  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), 'aerial-prepare-cand-test-'));
  const userDir = path.join(tmpBase, 'user');
  const coreDir = path.join(tmpBase, 'core');
  const targetDir = path.join(tmpBase, 'target');
  const localConfig = path.join(tmpBase, 'local.yaml');
  const fallbackConfig = path.join(tmpBase, 'fallback.yaml');

  fs.mkdirSync(userDir, { recursive: true });
  fs.mkdirSync(coreDir, { recursive: true });
  fs.mkdirSync(targetDir, { recursive: true });

  // localConfig has only comments (evaluates to null/empty)
  fs.writeFileSync(localConfig, '# Config Hash: 1234567890abcdef\n', 'utf8');
  // fallbackConfig has actual values
  fs.writeFileSync(fallbackConfig, 'port: "3002"\nallowed_hosts: "*"\n', 'utf8');

  const origConfigPath = process.env.CONFIG_PATH;
  try {
    // Point CONFIG_PATH to localConfig, but when it is comment-only, we want to test parseYaml behavior
    const yaml = resolveYamlParser();
    if (yaml) {
      const parsedLocal = parseYaml(yaml, fs.readFileSync(localConfig, 'utf8'), localConfig);
      assert.equal(parsedLocal, null);

      const parsedFallback = parseYaml(yaml, fs.readFileSync(fallbackConfig, 'utf8'), fallbackConfig);
      assert.deepEqual(parsedFallback, { port: '3002', allowed_hosts: '*' });
    }
  } finally {
    if (origConfigPath !== undefined) {
      process.env.CONFIG_PATH = origConfigPath;
    } else {
      delete process.env.CONFIG_PATH;
    }
    fs.rmSync(tmpBase, { recursive: true, force: true });
  }
});
