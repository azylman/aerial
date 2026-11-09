// Runs callback after the browser has painted the current frame: requestAnimationFrame fires just
// before the paint, and a setTimeout(0) queued from it runs once that paint is done. Returns a
// cancel function that stops whichever of the two is still pending.
export function afterNextPaint(callback: () => void): () => void {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const frame = requestAnimationFrame(() => {
    timeout = setTimeout(callback, 0);
  });
  return () => {
    cancelAnimationFrame(frame);
    clearTimeout(timeout);
  };
}
