import { memo, useEffect, useRef, useState } from 'react';
import { Glyph } from './Glyph';

/** How long the button shows its copied state (copyCronPrompt's revert delay). */
export const COPIED_MS = 1800;

export interface CopyButtonProps {
  /** Text written to the clipboard; an empty string makes the click a no-op. */
  readonly text: string;
  readonly ariaLabel: string;
  readonly className: string;
}

/**
 * Copy button: writes `text` to the clipboard and flips to a
 * transient COPIED state (`<className> copied`). The text arrives as a prop, so there is no
 * index lookup into a filtered list and no DOM write from outside React.
 */
export const CopyButton = memo(function CopyButton({ text, ariaLabel, className }: CopyButtonProps) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(
    () => () => {
      if (timer.current !== null) clearTimeout(timer.current);
    },
    [],
  );

  const copy = async (): Promise<void> => {
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
    } catch (err) {
      console.warn('Clipboard write failed:', err);
      return;
    }
    setCopied(true);
    if (timer.current !== null) clearTimeout(timer.current);
    timer.current = setTimeout(() => {
      timer.current = null;
      setCopied(false);
    }, COPIED_MS);
  };

  return (
    <button
      type="button"
      className={copied ? `${className} copied` : className}
      aria-label={ariaLabel}
      onClick={() => void copy()}
    >
      {copied ? (
        <>
          <Glyph name="check" /> COPIED
        </>
      ) : (
        <>
          <Glyph name="clipboard" /> COPY
        </>
      )}
    </button>
  );
});
