import { describe, expect, it } from 'vitest';
import { describeTextMismatch, normalizeParityText } from './parity.ts';

describe('normalizeParityText', () => {
  it.each([
    { name: 'collapses whitespace runs and trims', input: '  QUEUE\n\n  CONCURRENCY\t3 / 4  ', want: 'QUEUE CONCURRENCY 3 / 4' },
    { name: 'drops status glyph prefixes', input: '⚡ RUNNING ⏳ PENDING ✓ MERGED ✕ FAILED ⊘ CANCELLED', want: 'RUNNING PENDING MERGED FAILED CANCELLED' },
    { name: 'drops emoji prefixes', input: '📅 Every day 🎯 Once ⏰ Next 📋 COPY 🔄 RETRY 🚨 ALERT', want: 'Every day Once Next COPY RETRY ALERT' },
    { name: 'drops chrome symbols', input: '⏱ 3m PR ↗ ▸ INSTRUCTIONS 🔍 search ⟳ ×', want: '3m PR INSTRUCTIONS search' },
    { name: 'drops chrome emoji', input: '🧠 MEMORY 🛠️ BUILDS 🚀 PIPELINE 🟢 SYNC 🟡 BEHIND 💬 DISCORD 📦 repo 🕒 12:00', want: 'MEMORY BUILDS PIPELINE SYNC BEHIND DISCORD repo 12:00' },
    { name: 'drops pipeline and empty-state glyphs', input: '⚠️ DEGRADED ⬇️ FLUX ⚙️ CI 🩺 Health 🌌 ○ PENDING ▾', want: 'DEGRADED FLUX CI Health PENDING' },
    { name: 'keeps glyphs React renders as text', input: '🛰 Console • GIT SYNC (Δ 0s) Mon–Fri', want: '🛰 Console • GIT SYNC (Δ 0s) Mon–Fri' },
    { name: 'drops variation selectors', input: '⏱️ 2s ✓︎ OK', want: '2s OK' },
    { name: 'keeps words that ran together', input: 'Queue Concurrency3 / 4 WORKERS', want: 'Queue Concurrency3 / 4 WORKERS' },
    { name: 'keeps other punctuation', input: 'OLLAMA + GEMINI 3.8 // LVL 3', want: 'OLLAMA + GEMINI 3.8 // LVL 3' },
    { name: 'empty text', input: ' \n ', want: '' },
  ])('$name', ({ input, want }) => {
    expect(normalizeParityText(input)).toBe(want);
  });
});

describe('describeTextMismatch', () => {
  it.each([
    { name: 'equal texts', vanilla: 'A B C', react: 'A B C', want: null },
    {
      name: 'words run together',
      vanilla: 'Queue Concurrency 3 / 4 WORKERS',
      react: 'Queue Concurrency3 / 4 WORKERS',
      want: 'text differs at word 2: vanilla "Concurrency", react "Concurrency3"\n  vanilla: "Queue Concurrency 3 / 4 WORKERS"\n  react:   "Queue Concurrency3 / 4 WORKERS"',
    },
    {
      name: 'react text ends early',
      vanilla: 'ONE TWO THREE',
      react: 'ONE TWO',
      want: 'text differs at word 3: vanilla "THREE", react "<end>"\n  vanilla: "ONE TWO THREE"\n  react:   "ONE TWO"',
    },
    {
      name: 'context is clipped to 20 characters before and 40 after the word',
      vanilla: 'aaaaaaaaaa bbbbbbbbbb cccccccccc X dddddddddd eeeeeeeeee ffffffffff gggggggggg',
      react: 'aaaaaaaaaa bbbbbbbbbb cccccccccc Y dddddddddd eeeeeeeeee ffffffffff gggggggggg',
      want:
        'text differs at word 4: vanilla "X", react "Y"\n' +
        '  vanilla: "bbbbbbbb cccccccccc X dddddddddd eeeeeeeeee ffffffffff ggggg"\n' +
        '  react:   "bbbbbbbb cccccccccc Y dddddddddd eeeeeeeeee ffffffffff ggggg"',
    },
  ])('$name', ({ vanilla, react, want }) => {
    expect(describeTextMismatch(vanilla, react)).toBe(want);
  });
});
