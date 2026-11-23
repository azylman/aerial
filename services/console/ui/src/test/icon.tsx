// A stand-in for components/Icon that records every render by name, which pins down exactly which
// components re-rendered. Adopt with:
//   vi.mock('../../components/Icon', () => import('../../test/icon'));
// and clear iconRenders in beforeEach.
export const iconRenders: string[] = [];

export function Icon({ name }: { name: string }) {
  iconRenders.push(name);
  return <svg data-icon={name} />;
}
