import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { Icon, type IconName } from './Icon';

interface Row {
  name: IconName;
  size?: number;
  className?: string;
  wantClass: string;
  wantSize: string;
}

const rows: Row[] = [
  { name: 'bolt', wantClass: 'icon', wantSize: '14' },
  { name: 'check', size: 20, className: 'status-icon', wantClass: 'icon status-icon', wantSize: '20' },
];

describe('Icon', () => {
  it.each(rows)('$name renders a decorative svg', ({ name, size, className, wantClass, wantSize }) => {
    const { container } = render(
      <Icon name={name} {...(size === undefined ? {} : { size })} {...(className === undefined ? {} : { className })} />,
    );
    const svg = container.firstElementChild;
    expect(svg?.tagName).toBe('svg');
    expect(svg?.getAttribute('class')).toBe(wantClass);
    expect(svg?.getAttribute('width')).toBe(wantSize);
    expect(svg?.getAttribute('aria-hidden')).toBe('true');
    expect(svg?.querySelector('path')?.getAttribute('d')).not.toBe('');
  });
});
