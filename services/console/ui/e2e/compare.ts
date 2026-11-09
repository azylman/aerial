// Pixel comparison for the parity suite: pads mismatched sizes (the padding counts as different),
// runs pixelmatch, and writes vanilla/react/diff PNGs plus a JSON summary to e2e/artifacts/.

import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import pixelmatch from 'pixelmatch';
import { PNG } from 'pngjs';

export const ARTIFACTS_DIR = join(import.meta.dirname, 'artifacts');

export interface DiffResult {
  readonly diffPixels: number;
  readonly percent: number;
  readonly sizes: { readonly vanilla: string; readonly react: string };
  readonly files: { readonly vanilla: string; readonly react: string; readonly diff: string };
}

function padTo(png: PNG, width: number, height: number): PNG {
  if (png.width === width && png.height === height) return png;
  const out = new PNG({ width, height });
  PNG.bitblt(png, out, 0, 0, png.width, png.height, 0, 0);
  return out;
}

/** Compares two screenshots; `percent` is differing pixels over the viewport area. */
export function compareShots(slug: string, vanillaPng: Buffer, reactPng: Buffer, viewportArea: number): DiffResult {
  const vanilla = PNG.sync.read(vanillaPng);
  const react = PNG.sync.read(reactPng);
  const width = Math.max(vanilla.width, react.width);
  const height = Math.max(vanilla.height, react.height);
  const a = padTo(vanilla, width, height);
  const b = padTo(react, width, height);
  const diff = new PNG({ width, height });
  const diffPixels = pixelmatch(a.data, b.data, diff.data, width, height, { threshold: 0.1 });

  mkdirSync(ARTIFACTS_DIR, { recursive: true });
  const files = {
    vanilla: join(ARTIFACTS_DIR, `${slug}.vanilla.png`),
    react: join(ARTIFACTS_DIR, `${slug}.react.png`),
    diff: join(ARTIFACTS_DIR, `${slug}.diff.png`),
  };
  writeFileSync(files.vanilla, vanillaPng);
  writeFileSync(files.react, reactPng);
  writeFileSync(files.diff, PNG.sync.write(diff));

  const result: DiffResult = {
    diffPixels,
    percent: (diffPixels / viewportArea) * 100,
    sizes: { vanilla: `${vanilla.width}x${vanilla.height}`, react: `${react.width}x${react.height}` },
    files,
  };
  writeFileSync(join(ARTIFACTS_DIR, `${slug}.json`), `${JSON.stringify(result, null, 2)}\n`);
  return result;
}
