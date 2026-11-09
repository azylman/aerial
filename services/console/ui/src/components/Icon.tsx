// Inline SVG icon set for console chrome. Every glyph is a 24x24 stroke path so icons inherit
// `currentColor` and scale with font size; there is no icon font or sprite request.

const PATHS = {
  alarm: 'M12 21a8 8 0 1 0 0-16 8 8 0 0 0 0 16zM12 9v4l2 2M5 3 2 6M22 6l-3-3',
  ban: 'M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18zM5.6 5.6l12.8 12.8',
  bolt: 'M13 2 4 14h7l-1 8 9-12h-7l1-8z',
  chat: 'M21 12a8 8 0 0 1-11.6 7.1L4 20l1-4.6A8 8 0 1 1 21 12z',
  check: 'M20 6 9 17l-5-5',
  hourglass: 'M6 2h12M6 22h12M7 2c0 5 10 5 10 10S7 17 7 22M17 2c0 5-10 5-10 10s10 5 10 10',
  timer: 'M12 22a8 8 0 1 0 0-16 8 8 0 0 0 0 16zM12 10v4M10 2h4',
  x: 'M18 6 6 18M6 6l12 12',
} as const satisfies Record<string, string>;

export type IconName = keyof typeof PATHS;

export interface IconProps {
  readonly name: IconName;
  /** Rendered width/height in CSS pixels; defaults to 1em-ish 14px chrome size. */
  readonly size?: number;
  readonly className?: string;
}

/** Decorative icon: always aria-hidden, so the accessible name comes from the owning control. */
export function Icon({ name, size = 14, className }: IconProps) {
  return (
    <svg
      className={className === undefined ? 'icon' : `icon ${className}`}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
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
