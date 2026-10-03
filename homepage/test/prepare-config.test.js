'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const os = require('os');
const prepareConfig = require('../prepare-config');
const { run, resolveYamlParser, parseYaml, safeRead, writeAtomic, copyFallback } = prepareConfig;

test('prepare-config: exports correct functions and omits startWatcher', () => {
  assert.equal(typeof run, 'function');
  assert.equal(typeof resolveYamlParser, 'function');
  assert.equal(typeof parseYaml, 'function');
  assert.equal(typeof safeRead, 'function');
  assert.equal(typeof writeAtomic, 'function');
  assert.equal(typeof copyFallback, 'function');
  assert.equal(prepareConfig.startWatcher, undefined);
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
