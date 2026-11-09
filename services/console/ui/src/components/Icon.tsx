// Inline SVG icon set for console chrome. Every glyph is a 24x24 path so icons inherit
// `currentColor` and scale with font size; there is no icon font or sprite request. One entry per
// emoji the vanilla UI used as chrome (static/app.js, static/index.html):
//   ⚡ bolt  🎯 target  ⏰ alarm  🧠 brain  🛠️ wrench  🚀 rocket  📋 clipboard  🔍 search  ⟳ refresh
//   🔄 sync  ↗ external  ⬇️ arrow-down  ✓ check  ✕/× x  ⏱ timer  ⏳ hourglass  💬 chat  📦 package
//   🟢/🟡 dot  🚨/⚠️ alert  ○ circle  ⊘ ban  🩺 pulse  🕒 clock  📅 calendar  🌌 sparkle  ⚙️ gear
//   ▾ chevron-down  ▸ chevron-right

const PATHS = {
  alarm: 'M12 21a8 8 0 1 0 0-16 8 8 0 0 0 0 16zM12 9v4l2 2M5 3 2 6M22 6l-3-3',
  alert: 'M12 9v4M12 17h.01M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z',
  'arrow-down': 'M12 5v14M19 12l-7 7-7-7',
  ban: 'M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18zM5.6 5.6l12.8 12.8',
  bolt: 'M13 2 4 14h7l-1 8 9-12h-7l1-8z',
  brain:
    'M12 5a3 3 0 0 0-6 1 3 3 0 0 0-2 5 3 3 0 0 0 1 5 3 3 0 0 0 7 2zM12 5a3 3 0 0 1 6 1 3 3 0 0 1 2 5 3 3 0 0 1-1 5 3 3 0 0 1-7 2zM12 5v13',
  calendar: 'M5 4h14a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2zM16 2v4M8 2v4M3 10h18',
  chat: 'M21 12a8 8 0 0 1-11.6 7.1L4 20l1-4.6A8 8 0 1 1 21 12z',
  check: 'M20 6 9 17l-5-5',
  'chevron-down': 'M6 9l6 6 6-6',
  'chevron-right': 'M9 6l6 6-6 6',
  circle: 'M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18z',
  clipboard: 'M9 2h6a1 1 0 0 1 1 1v2H8V3a1 1 0 0 1 1-1zM16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2',
  clock: 'M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18zM12 7v5l3 2',
  dot: 'M12 7a5 5 0 1 0 0 10 5 5 0 0 0 0-10z',
  download: 'M12 3v12M7 10l5 5 5-5M5 21h14',
  external: 'M7 17 17 7M7 7h10v10',
  gear: 'M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM19 12l2-1.5-2-3.5-2.4.8a7 7 0 0 0-1.6-.9L14.5 4h-5L9 6.9a7 7 0 0 0-1.6.9L5 7 3 10.5 5 12l-2 1.5L5 17l2.4-.8a7 7 0 0 0 1.6.9l.5 2.9h5l.5-2.9a7 7 0 0 0 1.6-.9L19 17l2-3.5z',
  hourglass: 'M6 2h12M6 22h12M7 2c0 5 10 5 10 10S7 17 7 22M17 2c0 5-10 5-10 10s10 5 10 10',
  package: 'M21 8 12 3 3 8v8l9 5 9-5V8zM3 8l9 5 9-5M12 13v8',
  pulse: 'M22 12h-4l-3 9L9 3l-3 9H2',
  refresh: 'M21 12a9 9 0 1 1-2.6-6.4L21 8M21 3v5h-5',
  rocket:
    'M4.5 16.5c-1.5 1.3-2 5-2 5s3.7-.5 5-2a2.1 2.1 0 0 0-3-3zM12 15l-3-3a22 22 0 0 1 2-4A13 13 0 0 1 22 2c0 2.7-.8 7.5-6 11a22 22 0 0 1-4 2zM9 12H4s.6-3 2-4c1.6-1.1 5 0 5 0M12 15v5s3-.6 4-2c1.1-1.6 0-5 0-5',
  search: 'M11 19a8 8 0 1 0 0-16 8 8 0 0 0 0 16zM21 21l-4.3-4.3',
  sparkle: 'M12 3l1.9 5.1L19 10l-5.1 1.9L12 17l-1.9-5.1L5 10l5.1-1.9L12 3z',
  sync: 'M3 12a9 9 0 0 1 15.4-6.4L21 8M21 3v5h-5M21 12a9 9 0 0 1-15.4 6.4L3 16M3 21v-5h5',
  target: 'M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20zM12 18a6 6 0 1 0 0-12 6 6 0 0 0 0 12zM12 14a2 2 0 1 0 0-4 2 2 0 0 0 0 4z',
  timer: 'M12 22a8 8 0 1 0 0-16 8 8 0 0 0 0 16zM12 10v4M10 2h4',
  wrench:
    'M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.8-3.8a6 6 0 0 1-7.9 7.9l-6.9 6.9a2.1 2.1 0 0 1-3-3l6.9-6.9a6 6 0 0 1 7.9-7.9z',
  x: 'M18 6 6 18M6 6l12 12',
} as const satisfies Record<string, string>;

export type IconName = keyof typeof PATHS;

/** Solid glyphs (status dots) fill instead of stroking. */
const FILLED: ReadonlySet<IconName> = new Set<IconName>(['dot']);

export interface IconProps {
  readonly name: IconName;
  /** Rendered width/height in CSS pixels; defaults to 1em-ish 14px chrome size. */
  readonly size?: number;
  readonly className?: string;
}

/** Decorative icon: always aria-hidden, so the accessible name comes from the owning control. */
export function Icon({ name, size = 14, className }: IconProps) {
  const filled = FILLED.has(name);
  return (
    <svg
      className={className === undefined ? 'icon' : `icon ${className}`}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill={filled ? 'currentColor' : 'none'}
      stroke={filled ? 'none' : 'currentColor'}
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      <path d={PATHS[name]} />
    </svg>
  );
}
