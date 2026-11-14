import { Icon, type IconName } from './Icon';

/**
 * Inline SVG prefix inside a badge or pill (`⚡ RUNNING`). The
 * owning element carries the text, so the glyph stays decorative.
 */
export function Glyph({ name }: { readonly name: IconName }) {
  return (
    <span className="glyph">
      <Icon name={name} size={12} />
    </span>
  );
}

/** Badge content: optional glyph, then the text (separated by a space). */
export function GlyphText({ icon, text }: { readonly icon: IconName | null; readonly text: string }) {
  if (icon === null) return text;
  return (
    <>
      <Glyph name={icon} /> {text}
    </>
  );
}
