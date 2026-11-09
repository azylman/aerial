import { describe, expect, it } from 'vitest';
import { reconcile } from './reconcile';

// Each row reconciles a fresh JSON-parsed `next` against `prev` (no shared references), then
// checks which paths of the result are the original `prev` references.
interface Row {
  name: string;
  prev: string;
  next: string;
  wantRootReused: boolean;
  // Dotted paths (array indexes as numbers) that must be identical to the same path in prev...
  reusedFromPrev: readonly (readonly [resultPath: string, prevPath: string])[];
  // ...and paths that must be fresh references.
  freshPaths: readonly string[];
}

const rows: Row[] = [
  {
    name: 'identical nested object returns prev',
    prev: '{"a":{"b":[1,2]},"c":"x"}',
    next: '{"a":{"b":[1,2]},"c":"x"}',
    wantRootReused: true,
    reusedFromPrev: [['a', 'a'], ['a.b', 'a.b']],
    freshPaths: [],
  },
  {
    name: 'changed scalar keeps unchanged siblings',
    prev: '{"system_time":"t1","services":[{"name":"eri","status":"up"}],"git_sync":{"status":"synced"}}',
    next: '{"system_time":"t2","services":[{"name":"eri","status":"up"}],"git_sync":{"status":"synced"}}',
    wantRootReused: false,
    reusedFromPrev: [['services', 'services'], ['git_sync', 'git_sync']],
    freshPaths: [],
  },
  {
    name: 'id-keyed list reorder reuses rows but not the array',
    prev: '{"rows":[{"id":"a","v":1},{"id":"b","v":2}]}',
    next: '{"rows":[{"id":"b","v":2},{"id":"a","v":1}]}',
    wantRootReused: false,
    reusedFromPrev: [['rows.0', 'rows.1'], ['rows.1', 'rows.0']],
    freshPaths: ['rows'],
  },
  {
    name: 'id-keyed list with one changed row',
    prev: '{"rows":[{"id":1,"v":1},{"id":2,"v":2},{"id":3,"v":3}]}',
    next: '{"rows":[{"id":1,"v":1},{"id":2,"v":9},{"id":3,"v":3}]}',
    wantRootReused: false,
    reusedFromPrev: [['rows.0', 'rows.0'], ['rows.2', 'rows.2']],
    freshPaths: ['rows', 'rows.1'],
  },
  {
    name: 'name-keyed list with prepended row',
    prev: '[{"name":"eri","s":"up"},{"name":"haro","s":"up"}]',
    next: '[{"name":"new","s":"up"},{"name":"eri","s":"up"},{"name":"haro","s":"up"}]',
    wantRootReused: false,
    reusedFromPrev: [['1', '0'], ['2', '1']],
    freshPaths: ['', '0'],
  },
  {
    name: 'removed row shrinks the array',
    prev: '[{"id":"a"},{"id":"b"}]',
    next: '[{"id":"a"}]',
    wantRootReused: false,
    reusedFromPrev: [['0', '0']],
    freshPaths: [''],
  },
  {
    name: 'unkeyed objects match by index',
    prev: '[{"step":"build"},{"step":"push"}]',
    next: '[{"step":"build"},{"step":"deploy"}]',
    wantRootReused: false,
    reusedFromPrev: [['0', '0']],
    freshPaths: ['', '1'],
  },
  {
    name: 'duplicate keys match the first prev occurrence',
    prev: '[{"id":"a","v":1},{"id":"a","v":2}]',
    next: '[{"id":"a","v":1},{"id":"a","v":1}]',
    wantRootReused: false,
    reusedFromPrev: [['0', '0'], ['1', '0']],
    freshPaths: [''],
  },
  {
    name: 'equal scalar arrays return prev',
    prev: '{"tags":["a","b"]}',
    next: '{"tags":["a","b"]}',
    wantRootReused: true,
    reusedFromPrev: [['tags', 'tags']],
    freshPaths: [],
  },
  {
    name: 'added key produces a new object',
    prev: '{"a":{"x":1}}',
    next: '{"a":{"x":1},"error":"boom"}',
    wantRootReused: false,
    reusedFromPrev: [['a', 'a']],
    freshPaths: [''],
  },
  {
    name: 'removed key produces a new object',
    prev: '{"a":{"x":1},"error":"boom"}',
    next: '{"a":{"x":1}}',
    wantRootReused: false,
    reusedFromPrev: [['a', 'a']],
    freshPaths: [''],
  },
  {
    name: 'renamed key with same count produces a new object',
    prev: '{"a":1,"b":{"x":1}}',
    next: '{"a":1,"c":{"x":1}}',
    wantRootReused: false,
    reusedFromPrev: [],
    freshPaths: ['', 'c'],
  },
  {
    name: 'null to list takes next',
    prev: '{"quick_launch_links":null}',
    next: '{"quick_launch_links":[{"name":"grafana"}]}',
    wantRootReused: false,
    reusedFromPrev: [],
    freshPaths: ['', 'quick_launch_links'],
  },
  {
    name: 'object to array takes next',
    prev: '{"v":{"0":"a"}}',
    next: '{"v":["a"]}',
    wantRootReused: false,
    reusedFromPrev: [],
    freshPaths: ['', 'v'],
  },
  {
    name: 'map of repos keyed by path reuses unchanged entries',
    prev: '{"repos":{"/share/aerial":{"sync_status":"synced"},"/share/aerial-config":{"sync_status":"synced"}}}',
    next: '{"repos":{"/share/aerial":{"sync_status":"synced"},"/share/aerial-config":{"sync_status":"lagging"}}}',
    wantRootReused: false,
    reusedFromPrev: [['repos./share/aerial', 'repos./share/aerial']],
    freshPaths: ['repos', 'repos./share/aerial-config'],
  },
];

function at(value: unknown, path: string): unknown {
  if (path === '') return value;
  return path.split('.').reduce<unknown>((cur, key) => (cur as Record<string, unknown>)[key], value);
}

describe('reconcile', () => {
  it.each(rows)('$name', ({ prev, next, wantRootReused, reusedFromPrev, freshPaths }) => {
    const prevValue: unknown = JSON.parse(prev);
    const nextValue: unknown = JSON.parse(next);

    const got = reconcile(prevValue, nextValue);

    expect(got).toEqual(nextValue);
    expect(got === prevValue).toBe(wantRootReused);
    for (const [resultPath, prevPath] of reusedFromPrev) {
      expect(at(got, resultPath), resultPath).toBe(at(prevValue, prevPath));
    }
    for (const path of freshPaths) {
      expect(at(got, path) === at(prevValue, path), `${path} should be fresh`).toBe(false);
    }
  });

  it.each([
    { name: 'same primitive', prev: 3, next: 3, want: 3 },
    { name: 'different primitive', prev: 3, next: 4, want: 4 },
    { name: 'NaN equals NaN', prev: Number.NaN, next: Number.NaN, want: Number.NaN },
    { name: 'null to object', prev: null, next: { a: 1 }, want: { a: 1 } },
  ])('primitives: $name', ({ prev, next, want }) => {
    expect(reconcile<unknown>(prev, next)).toEqual(want);
  });
});
