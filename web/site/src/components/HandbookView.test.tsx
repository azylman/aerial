import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { filterChapters, HANDBOOK_CHAPTERS } from '../data/chapters';
import { HandbookView } from './HandbookView';

describe('filterChapters helper', () => {
  it('returns all chapters when query is empty or whitespace', () => {
    expect(filterChapters(HANDBOOK_CHAPTERS, '')).toHaveLength(HANDBOOK_CHAPTERS.length);
    expect(filterChapters(HANDBOOK_CHAPTERS, '   ')).toHaveLength(HANDBOOK_CHAPTERS.length);
  });

  it('matches chapter by title and summary', () => {
    const byTitle = filterChapters(HANDBOOK_CHAPTERS, 'Autonomous Software Factory');
    expect(byTitle).toHaveLength(1);
    expect(byTitle[0]!.id).toBe('software-factory');

    const bySummary = filterChapters(HANDBOOK_CHAPTERS, 'and architecture philosophy');
    expect(bySummary).toHaveLength(1);
    expect(bySummary[0]!.id).toBe('overview');
    expect(bySummary[0]!.sections).toHaveLength(HANDBOOK_CHAPTERS[0]!.sections.length);
  });

  it('matches chapter by chapter number formatting', () => {
    const ch1 = filterChapters(HANDBOOK_CHAPTERS, 'chapter 1');
    expect(ch1).toHaveLength(1);
    expect(ch1[0]!.number).toBe(1);

    const ch2 = filterChapters(HANDBOOK_CHAPTERS, 'ch. 2');
    expect(ch2).toHaveLength(1);
    expect(ch2[0]!.number).toBe(2);

    const ch3 = filterChapters(HANDBOOK_CHAPTERS, 'ch 3');
    expect(ch3).toHaveLength(1);
    expect(ch3[0]!.number).toBe(3);

    // Fragments like 'apt' should not trigger numberMatch across all chapters
    const aptMatches = filterChapters(HANDBOOK_CHAPTERS, 'apt');
    expect(aptMatches.length).toBeLessThan(HANDBOOK_CHAPTERS.length);
  });

  it('matches section by title, lead, paragraphs, and bullets', () => {
    // Section title
    const bySectionTitle = filterChapters(HANDBOOK_CHAPTERS, 'Infisical Machine Identity');
    expect(bySectionTitle.length).toBeGreaterThan(0);
    expect(bySectionTitle.some((c) => c.id === 'getting-started')).toBe(true);

    // Section lead
    const bySectionLead = filterChapters(HANDBOOK_CHAPTERS, 'Open-loop chatbots generate text');
    expect(bySectionLead.length).toBeGreaterThan(0);
    expect(bySectionLead[0]!.id).toBe('overview');

    // Section paragraph
    const byParagraph = filterChapters(HANDBOOK_CHAPTERS, 'Single-Service Table Ownership');
    expect(byParagraph.length).toBeGreaterThan(0);
    expect(byParagraph.some((c) => c.id === 'application-platform')).toBe(true);

    // Section bullets
    const byBullet = filterChapters(HANDBOOK_CHAPTERS, 'Hermetic Verification');
    expect(byBullet.length).toBeGreaterThan(0);
    expect(byBullet[0]!.id).toBe('overview');
  });

  it('matches section by code snippet code and caption', () => {
    // Code snippet content
    const byCode = filterChapters(HANDBOOK_CHAPTERS, 'verify.sh --staged');
    expect(byCode.length).toBeGreaterThan(0);

    // Code snippet caption
    const byCaption = filterChapters(HANDBOOK_CHAPTERS, 'CoreDNS rewrite configuration');
    expect(byCaption.length).toBeGreaterThan(0);
    expect(byCaption[0]!.id).toBe('application-platform');
  });

  it('matches section by callout title and text', () => {
    // Callout title
    const byCalloutTitle = filterChapters(HANDBOOK_CHAPTERS, 'Adversarial Multi-Agent Review Panel');
    expect(byCalloutTitle.length).toBeGreaterThan(0);
    expect(byCalloutTitle[0]!.id).toBe('software-factory');

    // Callout text
    const byCalloutText = filterChapters(HANDBOOK_CHAPTERS, 'prevent Flux kustomize-controller');
    expect(byCalloutText.length).toBeGreaterThan(0);
    expect(byCalloutText.some((c) => c.id === 'getting-started')).toBe(true);
  });
});

describe('HandbookView component', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('renders default chapter 1 when chapterId is not provided or unknown', () => {
    render(<HandbookView />);

    expect(screen.getByTestId('handbook-view')).toBeTruthy();
    expect(screen.getByText('CHAPTER 1')).toBeTruthy();
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('Overview & Why Aerial');

    // Section table of contents exists
    const toc = screen.getByRole('navigation', { name: 'Section table of contents' });
    expect(within(toc).getByText('The Closed-Loop Thesis')).toBeTruthy();

    // Callout in Chapter 1
    expect(screen.getByTestId('callout-closed-loop-thesis')).toBeTruthy();

    // First chapter has no previous button
    expect(screen.queryByTestId('prev-chapter-btn')).toBeNull();
    const nextBtn = screen.getByTestId('next-chapter-btn');
    expect(nextBtn.getAttribute('href')).toBe('#/docs/getting-started');
  });

  it('renders specific requested chapter and pagination buttons', () => {
    render(<HandbookView chapterId="application-platform" />);

    expect(screen.getByText('CHAPTER 3')).toBeTruthy();
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe(
      'The Self-Hosted Application Platform'
    );

    // Both prev and next buttons exist for middle chapters with destination hrefs
    const prevBtn = screen.getByTestId('prev-chapter-btn');
    const nextBtn = screen.getByTestId('next-chapter-btn');
    expect(prevBtn.getAttribute('href')).toBe('#/docs/getting-started');
    expect(nextBtn.getAttribute('href')).toBe('#/docs/software-factory');
  });

  it('clears active search and displays routed chapter when chapterId changes', () => {
    const { rerender } = render(<HandbookView chapterId="overview" />);

    const searchInput = screen.getByTestId('handbook-search-input');
    fireEvent.change(searchInput, { target: { value: 'Ariga Atlas' } });

    // Displays software factory chapter from search
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe(
      'The Autonomous Software Factory'
    );

    // Changing chapterId to a non-matching chapter clears search and renders that chapter
    rerender(<HandbookView chapterId="getting-started" />);

    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe(
      'Getting Started & Two-Repo GitOps'
    );
    expect(screen.queryByTestId('handbook-clear-search-btn')).toBeNull();
  });

  it('clears active search when a pagination link is clicked so linked chapter appears', () => {
    render(<HandbookView chapterId="application-platform" />);

    const searchInput = screen.getByTestId('handbook-search-input');
    fireEvent.change(searchInput, { target: { value: 'Ariga Atlas' } });

    // Displays software factory chapter from search fallback
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe(
      'The Autonomous Software Factory'
    );

    // Clicking Previous points to chapter 3 (application-platform, the routed chapter)
    const prevBtn = screen.getByTestId('prev-chapter-btn');
    fireEvent.click(prevBtn);

    // Search query is cleared and chapter 3 is shown
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe(
      'The Self-Hosted Application Platform'
    );
    expect(screen.queryByTestId('handbook-clear-search-btn')).toBeNull();
  });

  it('filters chapters and sections via instant client-side search', () => {
    render(<HandbookView />);

    const searchInput = screen.getByTestId('handbook-search-input');
    fireEvent.change(searchInput, { target: { value: 'Ariga Atlas' } });

    // Shows search result count
    expect(screen.getByTestId('search-results-count').textContent).toContain('1 chapter matches');

    // Displays software factory chapter
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe(
      'The Autonomous Software Factory'
    );

    // Also test search matching multiple chapters
    fireEvent.change(searchInput, { target: { value: 'GitOps' } });
    expect(screen.getByTestId('search-results-count').textContent).toContain('chapters match');

    // Clear search button appears and resets search
    const clearBtn = screen.getByTestId('handbook-clear-search-btn');
    fireEvent.click(clearBtn);

    expect(screen.queryByTestId('handbook-clear-search-btn')).toBeNull();
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('Overview & Why Aerial');
  });

  it('shows empty search state when query matches zero topics', () => {
    render(<HandbookView />);

    const searchInput = screen.getByTestId('handbook-search-input');
    fireEvent.change(searchInput, { target: { value: 'nonexistent-topic-query-123' } });

    expect(screen.getByTestId('empty-search-state')).toBeTruthy();
    expect(screen.getByTestId('empty-search-state').textContent).toContain(
      'No topics match "nonexistent-topic-query-123".'
    );
    expect(screen.queryByTestId('handbook-nav-list')).toBeNull();
  });

  it('handles code copying successfully and restores state after timeout', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText,
      },
    });

    render(<HandbookView chapterId="overview" />);

    const copyBtn = screen.getByTestId('copy-btn-closed-loop-thesis-0');
    expect(copyBtn.textContent).toContain('Copy Code');

    await act(async () => {
      fireEvent.click(copyBtn);
    });

    expect(writeText).toHaveBeenCalledWith(
      expect.stringContaining('./scripts/verify.sh --staged')
    );
    expect(copyBtn.textContent).toContain('Copied to Clipboard!');
    expect(copyBtn.classList.contains('copied')).toBe(true);

    // Advance 1500ms (timer 1 has 1000ms remaining)
    act(() => {
      vi.advanceTimersByTime(1500);
    });
    expect(copyBtn.textContent).toContain('Copied to Clipboard!');

    // Trigger second copy while timeout is active to cover timeout reset
    await act(async () => {
      fireEvent.click(copyBtn);
    });
    expect(copyBtn.textContent).toContain('Copied to Clipboard!');

    // Advance another 1500ms (3000ms since first click; ensures timer 1 was cleared and did not reset state)
    act(() => {
      vi.advanceTimersByTime(1500);
    });
    expect(copyBtn.textContent).toContain('Copied to Clipboard!');
    expect(copyBtn.classList.contains('copied')).toBe(true);

    // Fast forward past the second timer's deadline
    act(() => {
      vi.advanceTimersByTime(1100);
    });

    expect(copyBtn.textContent).toContain('Copy Code');
    expect(copyBtn.classList.contains('copied')).toBe(false);
  });

  it('handles clipboard failure gracefully', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('Clipboard permission denied'));
    Object.assign(navigator, {
      clipboard: {
        writeText,
      },
    });

    render(<HandbookView chapterId="overview" />);

    const copyBtn = screen.getByTestId('copy-btn-closed-loop-thesis-0');

    await act(async () => {
      fireEvent.click(copyBtn);
    });

    expect(copyBtn.textContent).toContain('Copy failed, select manually');
    expect(copyBtn.classList.contains('failed')).toBe(true);

    act(() => {
      vi.advanceTimersByTime(2600);
    });

    expect(copyBtn.textContent).toContain('Copy Code');
    expect(copyBtn.classList.contains('failed')).toBe(false);
  });

  it('handles missing navigator.clipboard gracefully', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      configurable: true,
      writable: true,
    });

    render(<HandbookView chapterId="overview" />);

    const copyBtn = screen.getByTestId('copy-btn-closed-loop-thesis-0');

    await act(async () => {
      fireEvent.click(copyBtn);
    });

    expect(copyBtn.textContent).toContain('Copy failed, select manually');
    expect(copyBtn.classList.contains('failed')).toBe(true);
  });

  it('smooth scrolls on plain left-click in TOC, but preserves default navigation on modified clicks', () => {
    render(<HandbookView chapterId="overview" />);

    const toc = screen.getByRole('navigation', { name: 'Section table of contents' });
    const tocLink = within(toc).getByText('The Closed-Loop Thesis');

    const targetSection = document.getElementById('section-closed-loop-thesis');
    expect(targetSection).toBeTruthy();
    const scrollSpy = vi.fn();
    if (targetSection) {
      targetSection.scrollIntoView = scrollSpy;
    }

    // 1. Plain left-click: prevents default and calls smooth scrollIntoView
    const plainClick = new MouseEvent('click', { bubbles: true, cancelable: true, button: 0 });
    tocLink.dispatchEvent(plainClick);
    expect(plainClick.defaultPrevented).toBe(true);
    expect(scrollSpy).toHaveBeenCalledWith({ behavior: 'smooth' });

    // 2. Modified click with metaKey: allows default browser tab/window opening
    scrollSpy.mockClear();
    const metaClick = new MouseEvent('click', {
      bubbles: true,
      cancelable: true,
      button: 0,
      metaKey: true,
    });
    tocLink.dispatchEvent(metaClick);
    expect(metaClick.defaultPrevented).toBe(false);
    expect(scrollSpy).not.toHaveBeenCalled();

    // 3. Modified click with ctrlKey: allows default browser tab/window opening
    const ctrlClick = new MouseEvent('click', {
      bubbles: true,
      cancelable: true,
      button: 0,
      ctrlKey: true,
    });
    tocLink.dispatchEvent(ctrlClick);
    expect(ctrlClick.defaultPrevented).toBe(false);
    expect(scrollSpy).not.toHaveBeenCalled();

    // 4. Auxiliary click (e.g. middle-click button === 1): allows default navigation
    const middleClick = new MouseEvent('click', {
      bubbles: true,
      cancelable: true,
      button: 1,
    });
    tocLink.dispatchEvent(middleClick);
    expect(middleClick.defaultPrevented).toBe(false);
    expect(scrollSpy).not.toHaveBeenCalled();
  });
});
