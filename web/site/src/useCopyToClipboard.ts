import { useEffect, useRef, useState } from 'react';

export function useCopyToClipboard<T = string | number>(resetDelay = 2500) {
  const [copiedId, setCopiedId] = useState<T | null>(null);
  const [copyFailedId, setCopyFailedId] = useState<T | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const copy = async (text: string, id: T = ('default' as unknown as T)): Promise<boolean> => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error('Clipboard API not available');
      }
      await navigator.clipboard.writeText(text);
      setCopyFailedId(null);
      setCopiedId(id);
      timeoutRef.current = setTimeout(() => {
        setCopiedId(null);
        timeoutRef.current = null;
      }, resetDelay);
      return true;
    } catch {
      setCopiedId(null);
      setCopyFailedId(id);
      timeoutRef.current = setTimeout(() => {
        setCopyFailedId(null);
        timeoutRef.current = null;
      }, resetDelay);
      return false;
    }
  };

  return {
    copiedId,
    copyFailedId,
    copy,
    isCopied: (id: T = ('default' as unknown as T)) => copiedId === id,
    isFailed: (id: T = ('default' as unknown as T)) => copyFailedId === id,
  };
}
