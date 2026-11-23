'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const os = require('os');
const prepareConfig = require('../prepare-config');
const { run, resolveDefaultCoreDir, resolveDefaultUserDir, resolveDefaultAppConfigDir, copyFallback, composeParts, resolveThemeAsset } = prepareConfig;

const stubJsonYaml = { load: (str) => JSON.parse(str), dump: (obj) => JSON.stringify(obj) };

function makeTree(prefix, dirs = ['user', 'core', 'target']) {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  const out = { base };
  for (const d of dirs) {
    out[d] = path.join(base, d);
    fs.mkdirSync(out[d], { recursive: true });
  }
  return out;
}

test('prepare-config: omits startWatcher', () => {
  assert.equal(prepareConfig.startWatcher, undefined);
});

test('prepare-config: resolveDefaultCoreDir picks the first present candidate', () => {
  const tests = [
    { name: '/local mount', present: '/local/core-homepage/config', want: '/local/core-homepage/config' },
    { name: '/share checkout', present: '/share/aerial/services/homepage/config', want: '/share/aerial/services/homepage/config' },
    { name: 'image fallback', present: null, want: '/app/core-homepage/config' }
  ];
  for (const tc of tests) {
    assert.equal(resolveDefaultCoreDir((p) => p === tc.present), tc.want, tc.name);
  }
});

test('prepare-config: resolveDefaultUserDir picks the first present candidate', () => {
  const tests = [
    { name: '/local mount', present: '/local/user-homepage', want: '/local/user-homepage' },
    { name: 'share fallback', present: null, want: '/share/aerial-config/homepage' }
  ];
  for (const tc of tests) {
    assert.equal(resolveDefaultUserDir((p) => p === tc.present), tc.want, tc.name);
  }
});

test('prepare-config: resolveDefaultAppConfigDir returns /app/config only when present', () => {
  const tests = [
    { name: 'present', present: '/app/config', want: '/app/config' },
    { name: 'absent', present: null, want: null }
  ];
  for (const tc of tests) {
    assert.equal(resolveDefaultAppConfigDir((p) => p === tc.present), tc.want, tc.name);
  }
});

test('prepare-config: run mirrors configs and composed custom.css to appConfigDir', () => {
  const { base: tmpBase, user: userDir, core: coreDir, target: targetDir, 'app-config': appConfigDir } = makeTree('aerial-prepare-appcfg-', ['user', 'core', 'target', 'app-config']);

  try {
    fs.writeFileSync(path.join(coreDir, 'custom.css'), '/* stale theme */\nbody { background: #000; }\n', 'utf8');
    fs.writeFileSync(path.join(coreDir, 'settings.yaml'), '{"theme":"dark","color":"slate"}\n', 'utf8');
    fs.writeFileSync(path.join(userDir, 'settings.yaml'), '{"showStats":true}\n', 'utf8');
    fs.writeFileSync(path.join(coreDir, 'extra.js'), '// core extra js\n', 'utf8');
    fs.writeFileSync(path.join(appConfigDir, 'custom.css'), '/* stale theme */\n', 'utf8');
    fs.writeFileSync(path.join(appConfigDir, 'extra.js'), '', 'utf8');

    run({ coreDir, userDir, targetDir, appConfigDir, yaml: stubJsonYaml, candidatePaths: [] });

    assert.equal(fs.readFileSync(path.join(appConfigDir, 'custom.css'), 'utf8'), '/* stale theme */\nbody { background: #000; }\n');
    assert.equal(fs.readFileSync(path.join(targetDir, 'custom.css'), 'utf8'), '/* stale theme */\nbody { background: #000; }\n');
    const appSettings = JSON.parse(fs.readFileSync(path.join(appConfigDir, 'settings.yaml'), 'utf8'));
    assert.equal(appSettings.theme, 'dark');
    assert.equal(appSettings.showStats, true);
    assert.equal(fs.readFileSync(path.join(appConfigDir, 'extra.js'), 'utf8'), '// core extra js\n');
  } finally {
    fs.rmSync(tmpBase, { recursive: true, force: true });
  }
});

test('prepare-config: run generates runtime.env with generic HOMEPAGE_VAR_<KEY> entries and safe shell literals', () => {
  const { base: tmpBase, user: userDir, core: coreDir, target: targetDir } = makeTree('aerial-prepare-env-test-');
  const configFile = path.join(tmpBase, 'homepage.yaml');

  const mockConfig = {
    port: '3005',
    allowed_hosts: 'aerial.lan',
    log_level: 'debug',
    node_tls_reject_unauthorized: 0,
    unifi_url: 'https://unifi.lan:8443',
    unifi_user: 'aerial-bot',
    qnap_user: 'qnap-admin',
    custom_token: 'my-secret-key',
    refresh_seconds: 15,
    enable_feature: true,
    disable_feature: false,
    empty_str: '',
    nested_obj: { nested_key: 'value' },
    list_val: ['item1'],
    shell_syntax_token: 'abc$HOME $(whoami) `id`',
    quote_token: "it's \"tricky\"",
    'invalid key with space': 'skip-me',
    'invalid.key.dot': 'skip-me',
    'invalid-key-hyphen': 'skip-me'
  };


  fs.writeFileSync(configFile, JSON.stringify(mockConfig), 'utf8');

  try {
    run({ coreDir, userDir, targetDir, yaml: stubJsonYaml, candidatePaths: [configFile] });

    const envPath = path.join(targetDir, 'runtime.env');
    assert.ok(fs.existsSync(envPath), 'runtime.env should be generated');

    const envContent = fs.readFileSync(envPath, 'utf8');
    const lines = envContent.trim().split('\n');

    assert.ok(lines.includes("PORT='3005'"));
    assert.ok(lines.includes("HOMEPAGE_ALLOWED_HOSTS='aerial.lan'"));
    assert.ok(lines.includes("LOG_LEVEL='debug'"));
    assert.ok(lines.includes("NODE_TLS_REJECT_UNAUTHORIZED='0'"));
    assert.ok(lines.includes("HOMEPAGE_VAR_UNIFI_URL='https://unifi.lan:8443'"));
    assert.ok(lines.includes("HOMEPAGE_VAR_UNIFI_USER='aerial-bot'"));
    assert.ok(lines.includes("HOMEPAGE_VAR_QNAP_USER='qnap-admin'"));
    assert.ok(lines.includes("HOMEPAGE_VAR_CUSTOM_TOKEN='my-secret-key'"));
    assert.ok(lines.includes("HOMEPAGE_VAR_REFRESH_SECONDS='15'"));
    assert.ok(lines.includes("HOMEPAGE_VAR_ENABLE_FEATURE='true'"));
    assert.ok(lines.includes("HOMEPAGE_VAR_DISABLE_FEATURE='false'"));
    assert.ok(lines.includes("HOMEPAGE_VAR_SHELL_SYNTAX_TOKEN='abc$HOME $(whoami) `id`'"));
    assert.ok(lines.includes("HOMEPAGE_VAR_QUOTE_TOKEN='it'\\''s \"tricky\"'"));

    assert.ok(!envContent.includes('HOMEPAGE_VAR_PORT'));
    assert.ok(!envContent.includes('EMPTY_STR'));
    assert.ok(!envContent.includes('NESTED_OBJ'));
    assert.ok(!envContent.includes('LIST_VAL'));
    assert.ok(!envContent.includes('INVALID KEY WITH SPACE'));
    assert.ok(!envContent.includes('INVALID.KEY.DOT'));
    assert.ok(!envContent.includes('INVALID-KEY-HYPHEN'));

    // Verify sourcing in POSIX /bin/sh preserves literal values without shell expansion
    const { execFileSync } = require('child_process');
    const shScript = `set -a; . "${envPath}"; set +a; printf '%s|%s' "$HOMEPAGE_VAR_SHELL_SYNTAX_TOKEN" "$HOMEPAGE_VAR_QUOTE_TOKEN"`;
    const shOut = execFileSync('/bin/sh', ['-c', shScript], { encoding: 'utf8' });
    assert.equal(shOut, `abc$HOME $(whoami) \`id\`|it's "tricky"`);
  } finally {
    fs.rmSync(tmpBase, { recursive: true, force: true });
  }
});

test('prepare-config: loads cleanly and merges core and user configs when /local/core-homepage/config is used', () => {
  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), 'aerial-prepare-local-core-'));
  const localCoreDir = path.join(tmpBase, 'local-core');
  const localCoreConfigDir = path.join(localCoreDir, 'config');
  const userDir = path.join(tmpBase, 'user');
  const targetDir = path.join(tmpBase, 'target');

  fs.mkdirSync(localCoreDir, { recursive: true });
  fs.mkdirSync(localCoreConfigDir, { recursive: true });
  fs.mkdirSync(userDir, { recursive: true });
  fs.mkdirSync(targetDir, { recursive: true });

  // Simulate ConfigMap flat mount of merge.js alongside prepare-config.js in localCoreDir
  const origPrepareSrc = fs.readFileSync(path.join(__dirname, '../prepare-config.js'), 'utf8');
  const mergeSrc = fs.readFileSync(path.join(__dirname, '../lib/merge.js'), 'utf8');

  fs.writeFileSync(path.join(localCoreDir, 'prepare-config.js'), origPrepareSrc, 'utf8');
  fs.writeFileSync(path.join(localCoreDir, 'merge.js'), mergeSrc, 'utf8');

  try {
    // Write sample core config and user config
    fs.writeFileSync(path.join(localCoreConfigDir, 'settings.yaml'), '{"theme":"dark","color":"slate"}\n', 'utf8');
    fs.writeFileSync(path.join(userDir, 'settings.yaml'), '{"showStats":true}\n', 'utf8');

    // Require the prepare-config in localCoreDir to verify it resolved ./merge when ./lib/merge.js is absent
    assert.equal(fs.existsSync(path.join(localCoreDir, 'lib', 'merge.js')), false);
    const localPrepare = require(path.join(localCoreDir, 'prepare-config.js'));


    localPrepare.run({
      coreDir: localCoreConfigDir,
      userDir,
      targetDir,
      yaml: stubJsonYaml,
      candidatePaths: []
    });

    const mergedSettings = JSON.parse(fs.readFileSync(path.join(targetDir, 'settings.yaml'), 'utf8'));
    assert.equal(mergedSettings.theme, 'dark');
    assert.equal(mergedSettings.showStats, true);
  } finally {
    fs.rmSync(tmpBase, { recursive: true, force: true });
  }
});

test('prepare-config: composeParts joins present parts and skips missing ones', async (t) => {
  const tests = [
    { name: 'css parts trimmed and newline-joined', parts: ['a {}\n', 'b {}', 'c {}\n\n'], separator: '\n', want: 'a {}\nb {}\nc {}\n' },
    { name: 'js parts semicolon-separated', parts: ['x();\n', 'y();'], separator: '\n;\n', want: 'x();\n;\ny();\n' },
    { name: 'missing and empty parts skipped', parts: [null, 'b', ''], separator: '\n', want: 'b\n' },
    { name: 'nothing present', parts: [null, undefined, '  \n'], separator: '\n', want: '' }
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      assert.equal(composeParts(tc.parts, tc.separator), tc.want);
    });
  }
});

test('prepare-config: resolveThemeAsset prefers the core mount, then the repo web/theme dir', async (t) => {
  const coreDir = '/srv/repo/services/homepage/config';
  const tests = [
    { name: 'core mount', present: ['/srv/repo/services/homepage/config/aerial-tokens.css'], want: '/srv/repo/services/homepage/config/aerial-tokens.css' },
    { name: 'repo layout', present: ['/srv/repo/web/theme/aerial-tokens.css'], want: '/srv/repo/web/theme/aerial-tokens.css' },
    { name: 'both present, core wins', present: ['/srv/repo/web/theme/aerial-tokens.css', '/srv/repo/services/homepage/config/aerial-tokens.css'], want: '/srv/repo/services/homepage/config/aerial-tokens.css' },
    { name: 'absent', present: [], want: null }
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      assert.equal(resolveThemeAsset(coreDir, 'aerial-tokens.css', (p) => tc.present.includes(p)), tc.want);
    });
  }
});

const CORE_REL = 'repo/services/homepage/config';
function writeFixtureTree(base, files) {
  for (const [rel, content] of Object.entries(files)) {
    const full = path.join(base, rel);
    fs.mkdirSync(path.dirname(full), { recursive: true });
    fs.writeFileSync(full, content, 'utf8');
  }
}

const composeCases = [
  {
    name: 'no user files',
    files: { [`${CORE_REL}/aerial-tokens.css`]: ':root{--ae:1}\n', [`${CORE_REL}/aerial-theme.js`]: 'window.T=1;\n', [`${CORE_REL}/custom.css`]: 'html.dark{}\n', [`${CORE_REL}/custom.js`]: 'core();\n' },
    wantCss: ':root{--ae:1}\nhtml.dark{}\n',
    wantJs: 'window.T=1;\n;\ncore();\n'
  },
  {
    name: 'user files appended after core',
    files: { [`${CORE_REL}/aerial-tokens.css`]: ':root{--ae:1}\n', [`${CORE_REL}/aerial-theme.js`]: 'window.T=1;\n', [`${CORE_REL}/custom.css`]: 'html.dark{}\n', [`${CORE_REL}/custom.js`]: 'core();\n', 'user/custom.css': 'body{}\n', 'user/custom.js': 'user();\n' },
    wantCss: ':root{--ae:1}\nhtml.dark{}\nbody{}\n',
    wantJs: 'window.T=1;\n;\ncore();\n;\nuser();\n'
  },
  {
    name: 'shared sources from repo web/theme',
    files: { 'repo/web/theme/aerial-tokens.css': ':root{--ae:2}\n', 'repo/web/theme/aerial-theme.js': 'window.T=2;\n', [`${CORE_REL}/custom.css`]: 'html.dark{}\n', [`${CORE_REL}/custom.js`]: 'core();\n' },
    wantCss: ':root{--ae:2}\nhtml.dark{}\n',
    wantJs: 'window.T=2;\n;\ncore();\n'
  },
  {
    name: 'shared sources absent',
    files: { [`${CORE_REL}/custom.css`]: 'html.dark{}\n', 'user/custom.js': 'user();\n' },
    wantCss: 'html.dark{}\n',
    wantJs: 'user();\n'
  },
  {
    name: 'nothing present',
    files: {},
    wantCss: '',
    wantJs: ''
  }
];

test('prepare-config: run writes composed custom.css and custom.js to every target', async (t) => {
  for (const tc of composeCases) {
    await t.test(tc.name, () => {
      const base = fs.mkdtempSync(path.join(os.tmpdir(), 'aerial-prepare-compose-'));
      try {
        writeFixtureTree(base, tc.files);
        const coreDir = path.join(base, CORE_REL);
        const userDir = path.join(base, 'user');
        const targetDir = path.join(base, 'target');
        const appConfigDir = path.join(base, 'app-config');
        fs.mkdirSync(coreDir, { recursive: true });
        fs.mkdirSync(userDir, { recursive: true });

        run({ coreDir, userDir, targetDir, appConfigDir, yaml: stubJsonYaml, candidatePaths: [] });

        for (const dir of [targetDir, appConfigDir]) {
          assert.equal(fs.readFileSync(path.join(dir, 'custom.css'), 'utf8'), tc.wantCss, `${tc.name}: custom.css in ${path.basename(dir)}`);
          assert.equal(fs.readFileSync(path.join(dir, 'custom.js'), 'utf8'), tc.wantJs, `${tc.name}: custom.js in ${path.basename(dir)}`);
          assert.equal(fs.existsSync(path.join(dir, 'aerial-tokens.css')), false, `${tc.name}: aerial-tokens.css not raw-copied`);
          assert.equal(fs.existsSync(path.join(dir, 'aerial-theme.js')), false, `${tc.name}: aerial-theme.js not raw-copied`);
        }
      } finally {
        fs.rmSync(base, { recursive: true, force: true });
      }
    });
  }
});

test('prepare-config: copyFallback copies raw configs and writes composed custom.css and custom.js', async (t) => {
  for (const tc of composeCases) {
    await t.test(tc.name, () => {
      const base = fs.mkdtempSync(path.join(os.tmpdir(), 'aerial-prepare-fallback-'));
      try {
        writeFixtureTree(base, { ...tc.files, [`${CORE_REL}/settings.yaml`]: 'color: slate\n' });
        const dstDir = path.join(base, 'dst');
        fs.mkdirSync(dstDir, { recursive: true });

        copyFallback(path.join(base, CORE_REL), dstDir, path.join(base, 'user'));

        assert.equal(fs.readFileSync(path.join(dstDir, 'settings.yaml'), 'utf8'), 'color: slate\n', `${tc.name}: settings.yaml`);
        assert.equal(fs.readFileSync(path.join(dstDir, 'custom.css'), 'utf8'), tc.wantCss, `${tc.name}: custom.css`);
        assert.equal(fs.readFileSync(path.join(dstDir, 'custom.js'), 'utf8'), tc.wantJs, `${tc.name}: custom.js`);
        assert.equal(fs.existsSync(path.join(dstDir, 'aerial-tokens.css')), false, `${tc.name}: aerial-tokens.css not raw-copied`);
        assert.equal(fs.existsSync(path.join(dstDir, 'aerial-theme.js')), false, `${tc.name}: aerial-theme.js not raw-copied`);
      } finally {
        fs.rmSync(base, { recursive: true, force: true });
      }
    });
  }
});
