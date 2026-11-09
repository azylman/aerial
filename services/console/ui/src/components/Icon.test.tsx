import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { Icon, type IconName } from './Icon';

interface Row {
  name: IconName;
  size?: number;
  className?: string;
  wantClass: string;
  wantSize: string;
  wantFill: string;
  wantStroke: string;
}

const rows: Row[] = [
  { name: 'bolt', wantClass: 'icon', wantSize: '14', wantFill: 'none', wantStroke: 'currentColor' },
  {
    name: 'check',
    size: 20,
    className: 'status-icon',
    wantClass: 'icon status-icon',
    wantSize: '20',
    wantFill: 'none',
    wantStroke: 'currentColor',
  },
  { name: 'dot', size: 10, wantClass: 'icon', wantSize: '10', wantFill: 'currentColor', wantStroke: 'none' },
];

describe('Icon', () => {
  it.each(rows)('$name renders a decorative svg', ({ name, size, className, wantClass, wantSize, wantFill, wantStroke }) => {
    const { container } = render(
      <Icon name={name} {...(size === undefined ? {} : { size })} {...(className === undefined ? {} : { className })} />,
    );
    const svg = container.firstElementChild;
    expect(svg?.tagName).toBe('svg');
    expect(svg?.getAttribute('class')).toBe(wantClass);
    expect(svg?.getAttribute('width')).toBe(wantSize);
    expect(svg?.getAttribute('fill')).toBe(wantFill);
    expect(svg?.getAttribute('stroke')).toBe(wantStroke);
    expect(svg?.getAttribute('aria-hidden')).toBe('true');
    expect(svg?.querySelector('path')?.getAttribute('d')).not.toBe('');
  });
});
