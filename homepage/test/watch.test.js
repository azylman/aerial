'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { startWatcher } = require('../prepare-config');

test('startWatcher: initializes and closes cleanly', () => {
  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), 'aerial-watch-test-'));
  const userDir = path.join(tmpBase, 'user');
  const coreDir = path.join(tmpBase, 'core');
  const targetDir = path.join(tmpBase, 'target');

  fs.mkdirSync(userDir, { recursive: true });
  fs.mkdirSync(coreDir, { recursive: true });
  fs.mkdirSync(targetDir, { recursive: true });

  const watcher = startWatcher({
    coreDir,
    userDir,
    targetDir,
    pollIntervalMs: 0,
    debounceMs: 50
  });

  assert.ok(watcher);
  assert.equal(typeof watcher.close, 'function');

  watcher.close();
  fs.rmSync(tmpBase, { recursive: true, force: true });
});

test('startWatcher: triggers recompile on file modification', async () => {
  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), 'aerial-watch-test-'));
  const userDir = path.join(tmpBase, 'user');
  const coreDir = path.join(tmpBase, 'core');
  const targetDir = path.join(tmpBase, 'target');

  fs.mkdirSync(userDir, { recursive: true });
  fs.mkdirSync(coreDir, { recursive: true });
  fs.mkdirSync(targetDir, { recursive: true });

  let recompileCount = 0;

  const watcher = startWatcher({
    coreDir,
    userDir,
    targetDir,
    pollIntervalMs: 50,
    debounceMs: 50,
    onRecompile: () => {
      recompileCount++;
    }
  });

  try {
    // 1. Create a new file
    const testFile = path.join(userDir, 'custom.css');
    fs.writeFileSync(testFile, '/* initial */', 'utf8');

    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.ok(recompileCount >= 1, `Expected recompile on file creation, got ${recompileCount}`);

    const countAfterCreate = recompileCount;

    // 2. Modify the file (advance mtime)
    fs.writeFileSync(testFile, '/* updated content */', 'utf8');
    const futureTime = new Date(Date.now() + 2000);
    fs.utimesSync(testFile, futureTime, futureTime);

    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.ok(recompileCount > countAfterCreate, `Expected recompile on file update, got ${recompileCount}`);

    const countAfterUpdate = recompileCount;

    // 3. Delete the file
    fs.unlinkSync(testFile);

    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.ok(recompileCount > countAfterUpdate, `Expected recompile on file delete, got ${recompileCount}`);
  } finally {
    watcher.close();
    fs.rmSync(tmpBase, { recursive: true, force: true });
  }
});

test('startWatcher: debounces rapid sequential modifications', async () => {
  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), 'aerial-watch-test-'));
  const userDir = path.join(tmpBase, 'user');
  const coreDir = path.join(tmpBase, 'core');
  const targetDir = path.join(tmpBase, 'target');

  fs.mkdirSync(userDir, { recursive: true });
  fs.mkdirSync(coreDir, { recursive: true });
  fs.mkdirSync(targetDir, { recursive: true });

  let recompileCount = 0;

  const watcher = startWatcher({
    coreDir,
    userDir,
    targetDir,
    pollIntervalMs: 20,
    debounceMs: 150,
    onRecompile: () => {
      recompileCount++;
    }
  });

  try {
    const file1 = path.join(userDir, 'services.yaml');
    const file2 = path.join(userDir, 'widgets.yaml');

    // Rapid sequential writes within debounce window
    fs.writeFileSync(file1, 'content 1', 'utf8');
    await new Promise((resolve) => setTimeout(resolve, 30));
    fs.writeFileSync(file2, 'content 2', 'utf8');
    await new Promise((resolve) => setTimeout(resolve, 30));
    fs.writeFileSync(file1, 'content 1 updated', 'utf8');

    // Wait for debounce window to fire
    await new Promise((resolve) => setTimeout(resolve, 300));

    // Debounce should coalesce rapid writes into 1 recompile (or at most 2 if poll fired during window)
    assert.ok(recompileCount >= 1 && recompileCount <= 2, `Expected 1 or 2 recompiles, got ${recompileCount}`);
  } finally {
    watcher.close();
    fs.rmSync(tmpBase, { recursive: true, force: true });
  }
});
