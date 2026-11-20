import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { getAgentPaletteIndex } from '../lib/formatters';
import { AgentBadge } from './AgentBadge';

describe('AgentBadge', () => {
  afterEach(cleanup);

  it('renders default aerial badge when agent is empty or undefined', () => {
    const { rerender } = render(<AgentBadge />);
    let el = screen.getByText('aerial');
    expect(el.className).toBe('agent-badge agent-aerial');
    expect(el.getAttribute('data-agent')).toBe('aerial');
    expect(el.getAttribute('data-agent-palette')).toBeNull();

    rerender(<AgentBadge agent="" />);
    el = screen.getByText('aerial');
    expect(el.className).toBe('agent-badge agent-aerial');
    expect(el.getAttribute('data-agent')).toBe('aerial');
    expect(el.getAttribute('data-agent-palette')).toBeNull();

    rerender(<AgentBadge agent={null} />);
    el = screen.getByText('aerial');
    expect(el.className).toBe('agent-badge agent-aerial');
    expect(el.getAttribute('data-agent')).toBe('aerial');
    expect(el.getAttribute('data-agent-palette')).toBeNull();
  });

  it('renders explicit aerial badge without palette attributes', () => {
    render(<AgentBadge agent="aerial" />);
    const el = screen.getByText('aerial');
    expect(el.className).toBe('agent-badge agent-aerial');
    expect(el.getAttribute('data-agent')).toBe('aerial');
    expect(el.getAttribute('data-agent-palette')).toBeNull();
  });

  it('renders non-aerial agent badge with deterministic palette index', () => {
    const agent = 'agent-alpha';
    const palette = getAgentPaletteIndex(agent);
    render(<AgentBadge agent={agent} />);
    const el = screen.getByText(agent);
    expect(el.className).toBe(`agent-badge agent-${agent} agent-palette-${palette}`);
    expect(el.getAttribute('data-agent')).toBe(agent);
    expect(el.getAttribute('data-agent-palette')).toBe(String(palette));
  });

  it('appends extra className when provided', () => {
    render(<AgentBadge agent="aerial" className="custom-extra" />);
    const el = screen.getByText('aerial');
    expect(el.className).toBe('agent-badge agent-aerial custom-extra');
  });
});
