'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  mergeServices,
  mergeBookmarks,
  mergeWidgets,
  mergeSettings,
  mergeDocker,
  mergeCustomCss
} = require('../lib/merge');

test('mergeServices: handles null, undefined, non-array, and empty inputs gracefully', () => {
  assert.deepEqual(mergeServices(null, null), []);
  assert.deepEqual(mergeServices(undefined, undefined), []);
  assert.deepEqual(mergeServices('invalid', {}), []);
  assert.deepEqual(mergeServices([], []), []);
  assert.deepEqual(mergeServices([{ GroupA: [{ Svc1: { href: 'http://a' } }] }], null), [
    { GroupA: [{ Svc1: { href: 'http://a' } }] }
  ]);
  assert.deepEqual(mergeServices(null, [{ GroupB: [{ Svc2: { href: 'http://b' } }] }]), [
    { GroupB: [{ Svc2: { href: 'http://b' } }] }
  ]);
});

test('mergeServices: merges novel and matching groups with service deduplication', () => {
  const core = [
    {
      'Aerial AI & Mission Control': [
        { 'Aerial Command HUD': { href: '/dashboard/', icon: 'hud.png' } },
        { AgentsView: { href: '/agentsview/', icon: 'term.png' } }
      ]
    },
    {
      Observability: [
        { 'Grafana Telemetry': { href: '/grafana/', description: 'Core' } }
      ]
    }
  ];

  const user = [
    {
      Observability: [
        { 'Grafana Telemetry': { href: 'https://grafana.example.com', description: 'User Override' } },
        { Unpoller: { href: 'http://unpoller.local:9130' } }
      ]
    },
    {
      'Smart Home': [
        { 'Home Assistant': { href: 'https://home.example.com' } }
      ]
    }
  ];

  const merged = mergeServices(core, user);

  // Group count: 3 groups ('Aerial AI & Mission Control', 'Observability', 'Smart Home')
  assert.equal(merged.length, 3);
  assert.equal(Object.keys(merged[0])[0], 'Aerial AI & Mission Control');
  assert.equal(Object.keys(merged[1])[0], 'Observability');
  assert.equal(Object.keys(merged[2])[0], 'Smart Home');

  // Observability services: Grafana overridden, Unpoller appended
  const obsServices = merged[1].Observability;
  assert.equal(obsServices.length, 2);
  assert.deepEqual(obsServices[0], {
    'Grafana Telemetry': { href: 'https://grafana.example.com', description: 'User Override' }
  });
  assert.deepEqual(obsServices[1], {
    Unpoller: { href: 'http://unpoller.local:9130' }
  });

  // Core not mutated
  assert.equal(core[1].Observability.length, 1);
});

test('mergeBookmarks: preserves nested array structure and merges categories', () => {
  const core = [
    {
      Developer: [
        { 'GitHub (Core)': [{ href: 'https://github.com/example/core-engine', icon: 'github.png' }] }
      ]
    }
  ];

  const user = [
    {
      Developer: [
        { 'GitHub (Config)': [{ href: 'https://github.com/example/user-config', icon: 'github.png' }] }
      ]
    },
    {
      Cloud: [
        { 'Cloud Console': [{ href: 'https://cloud.example.com', icon: 'cloud.png' }] }
      ]
    }
  ];

  const merged = mergeBookmarks(core, user);
  assert.equal(merged.length, 2);
  assert.equal(Object.keys(merged[0])[0], 'Developer');
  assert.equal(Object.keys(merged[1])[0], 'Cloud');

  const devBookmarks = merged[0].Developer;
  assert.equal(devBookmarks.length, 2);
  assert.deepEqual(devBookmarks[0], {
    'GitHub (Core)': [{ href: 'https://github.com/example/core-engine', icon: 'github.png' }]
  });
  assert.deepEqual(devBookmarks[1], {
    'GitHub (Config)': [{ href: 'https://github.com/example/user-config', icon: 'github.png' }]
  });
});

test('mergeWidgets: concatenates lists and deduplicates singletons favoring user config', () => {
  const core = [
    { search: { provider: 'google', target: '_self' } },
    { resources: { cpu: true, memory: true } }
  ];

  const user = [
    { search: { provider: 'duckduckgo', target: '_blank' } },
    { openmeteo: { label: 'Local Weather', latitude: 40.7128, longitude: -74.0060 } }
  ];

  const merged = mergeWidgets(core, user);
  assert.equal(merged.length, 3);

  // search: user override
  assert.deepEqual(merged[0], { search: { provider: 'duckduckgo', target: '_blank' } });
  // resources: retained from core
  assert.deepEqual(merged[1], { resources: { cpu: true, memory: true } });
  // openmeteo: added from user
  assert.deepEqual(merged[2], { openmeteo: { label: 'Local Weather', latitude: 40.7128, longitude: -74.0060 } });
});

test('mergeSettings: deep merges layout and allows root styling overrides', () => {
  const core = {
    theme: 'dark',
    color: 'slate',
    cardBlur: 'md',
    cardOpacity: 85,
    headerStyle: 'clean',
    layout: {
      'Aerial AI & Mission Control': { style: 'row', columns: 4 },
      Observability: { style: 'row', columns: 2 }
    }
  };

  const user = {
    color: 'purple',
    layout: {
      'Smart Home': { style: 'row', columns: 2 },
      Observability: { style: 'row', columns: 3 }
    }
  };

  const merged = mergeSettings(core, user);
  assert.equal(merged.theme, 'dark');
  assert.equal(merged.color, 'purple'); // overridden by user
  assert.equal(merged.cardBlur, 'md');
  assert.equal(merged.cardOpacity, 85);
  assert.equal(merged.headerStyle, 'clean');

  // Layout merged
  assert.deepEqual(merged.layout, {
    'Aerial AI & Mission Control': { style: 'row', columns: 4 },
    Observability: { style: 'row', columns: 3 }, // overridden
    'Smart Home': { style: 'row', columns: 2 }   // appended
  });
});

test('mergeDocker: deep merges multi-host Docker connection objects', () => {
  const core = {
    'my-docker': {
      socket: '/var/run/docker.sock'
    }
  };

  const user = {
    'remote-docker': {
      host: '192.0.2.14',
      port: 2375
    }
  };

  const merged = mergeDocker(core, user);
  assert.deepEqual(merged, {
    'my-docker': { socket: '/var/run/docker.sock' },
    'remote-docker': { host: '192.0.2.14', port: 2375 }
  });
});

test('mergeCustomCss: concatenates core and user CSS with newline', () => {
  assert.equal(mergeCustomCss(null, null), '');
  assert.equal(mergeCustomCss('body { color: red; }', null), 'body { color: red; }');
  assert.equal(mergeCustomCss(null, 'body { color: blue; }'), 'body { color: blue; }');
  assert.equal(
    mergeCustomCss('/* Core CSS */\n:root { --bg: #000; }', '/* User CSS */\n:root { --bg: #111; }'),
    '/* Core CSS */\n:root { --bg: #000; }\n\n/* User CSS */\n:root { --bg: #111; }'
  );
});
