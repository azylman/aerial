import { type ChangeEvent, useEffect, useRef, useState } from 'react';
import {
  type ChapterCallout,
  type ChapterSection,
  type CodeSnippet,
  filterChapters,
  HANDBOOK_CHAPTERS,
  type HandbookChapter,
} from '../data/chapters';

export { filterChapters, HANDBOOK_CHAPTERS };
export type { ChapterCallout, ChapterSection, CodeSnippet, HandbookChapter };

export interface HandbookViewProps {
  readonly chapterId?: string | undefined;
}

export function HandbookView({ chapterId }: HandbookViewProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [copiedKey, setCopiedKey] = useState<string | null>(null);
  const [copyFailedKey, setCopyFailedKey] = useState<string | null>(null);
  const copyTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const prevChapterIdRef = useRef<string | undefined>(chapterId);

  useEffect(() => {
    if (prevChapterIdRef.current !== chapterId) {
      prevChapterIdRef.current = chapterId;
      setSearchQuery('');
    }
  }, [chapterId]);

  useEffect(() => {
    return () => {
      if (copyTimeoutRef.current) {
        clearTimeout(copyTimeoutRef.current);
      }
    };
  }, []);

  const handleCopy = async (key: string, code: string) => {
    if (copyTimeoutRef.current) {
      clearTimeout(copyTimeoutRef.current);
      copyTimeoutRef.current = null;
    }
    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error('Clipboard API not available');
      }
      await navigator.clipboard.writeText(code);
      setCopyFailedKey(null);
      setCopiedKey(key);
      copyTimeoutRef.current = setTimeout(() => {
        setCopiedKey(null);
        copyTimeoutRef.current = null;
      }, 2500);
    } catch {
      setCopiedKey(null);
      setCopyFailedKey(key);
      copyTimeoutRef.current = setTimeout(() => {
        setCopyFailedKey(null);
        copyTimeoutRef.current = null;
      }, 2500);
    }
  };

  const handleSearchChange = (event: ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(event.target.value);
  };

  const handleClearSearch = () => {
    setSearchQuery('');
  };

  const handleTocClick = (event: React.MouseEvent<HTMLAnchorElement>, sectionId: string) => {
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0) {
      return;
    }
    event.preventDefault();
    const target = document.getElementById(`section-${sectionId}`);
    if (target && typeof target.scrollIntoView === 'function') {
      target.scrollIntoView({ behavior: 'smooth' });
    }
  };

  const filteredChapters = filterChapters(HANDBOOK_CHAPTERS, searchQuery);

  const activeChapterId = chapterId || 'overview';
  const matchedChapter = HANDBOOK_CHAPTERS.find((ch) => ch.id === activeChapterId) || HANDBOOK_CHAPTERS[0]!;

  // If a search query is active, display matching sections of the active chapter if present,
  // or fall back to the first matching chapter in filtered list.
  const activeFiltered = filteredChapters.find((ch) => ch.id === matchedChapter.id);
  const displayChapter = activeFiltered || (filteredChapters.length > 0 ? filteredChapters[0]! : matchedChapter);

  // Pagination based on canonical full chapter list
  const canonicalIndex = HANDBOOK_CHAPTERS.findIndex((ch) => ch.id === displayChapter.id);
  const prevChapter = canonicalIndex > 0 ? HANDBOOK_CHAPTERS[canonicalIndex - 1] : null;
  const nextChapter = canonicalIndex < HANDBOOK_CHAPTERS.length - 1 ? HANDBOOK_CHAPTERS[canonicalIndex + 1] : null;

  return (
    <section className="handbook-view" data-testid="handbook-view">
      <div className="handbook-container">
        {/* Sticky Sidebar Navigation */}
        <aside className="handbook-sidebar" aria-label="Handbook chapters">
          <div className="handbook-sidebar-title">Deep-Dive Handbook</div>

          <div className="handbook-search-box">
            <input
              type="search"
              className="handbook-search-input"
              placeholder="Search handbook topics..."
              aria-label="Search handbook"
              value={searchQuery}
              onChange={handleSearchChange}
              data-testid="handbook-search-input"
            />
            {searchQuery && (
              <button
                type="button"
                className="handbook-clear-btn"
                onClick={handleClearSearch}
                aria-label="Clear search"
                data-testid="handbook-clear-search-btn"
              >
                ✕
              </button>
            )}
          </div>

          {searchQuery && (
            <div className="handbook-search-count" data-testid="search-results-count">
              {filteredChapters.length === 1
                ? '1 chapter matches'
                : `${filteredChapters.length} chapters match`}
            </div>
          )}

          {filteredChapters.length === 0 ? (
            <div className="handbook-empty-search" role="status" data-testid="empty-search-state">
              No topics match &quot;{searchQuery}&quot;.
            </div>
          ) : (
            <ul className="handbook-nav-list" data-testid="handbook-nav-list">
              {filteredChapters.map((ch) => {
                const isActive = ch.id === displayChapter.id;
                return (
                  <li key={ch.id}>
                    <a
                      href={`#/docs/${ch.id}`}
                      className={`handbook-nav-link ${isActive ? 'active' : ''}`}
                      aria-current={isActive ? 'page' : undefined}
                      onClick={handleClearSearch}
                      data-testid={`handbook-nav-link-${ch.id}`}
                    >
                      <span>Ch. {ch.number}:</span>
                      <span>{ch.title}</span>
                    </a>
                  </li>
                );
              })}
            </ul>
          )}
        </aside>

        {/* Main Article Content */}
        <article className="handbook-content" data-testid="handbook-article">
          <div className="chapter-badge">CHAPTER {displayChapter.number}</div>
          <h1 className="chapter-heading">{displayChapter.title}</h1>
          <div className="chapter-body">
            <p className="chapter-summary">{displayChapter.summary}</p>

            {/* Section Table of Contents */}
            {displayChapter.sections.length > 0 && (
              <nav className="section-toc" aria-label="Section table of contents">
                <div className="section-toc-title">On This Page</div>
                <ul className="section-toc-list">
                  {displayChapter.sections.map((sec) => (
                    <li key={sec.id}>
                      <a
                        href={`#section-${sec.id}`}
                        className="section-toc-link"
                        onClick={(e) => handleTocClick(e, sec.id)}
                      >
                        {sec.title}
                      </a>
                    </li>
                  ))}
                </ul>
              </nav>
            )}

            {/* Render Chapter Sections */}
            <div className="handbook-sections-stream">
              {displayChapter.sections.map((sec) => (
                <section
                  key={sec.id}
                  id={`section-${sec.id}`}
                  className="handbook-section"
                  aria-labelledby={`heading-${sec.id}`}
                  data-testid={`section-${sec.id}`}
                >
                  <h2 id={`heading-${sec.id}`} className="section-heading">
                    {sec.title}
                  </h2>

                  {sec.lead && <p className="section-lead">{sec.lead}</p>}

                  {sec.paragraphs.map((p, idx) => (
                    <p key={idx}>{p}</p>
                  ))}

                  {sec.bullets && sec.bullets.length > 0 && (
                    <ul className="section-bullets">
                      {sec.bullets.map((bullet, idx) => (
                        <li key={idx}>{bullet}</li>
                      ))}
                    </ul>
                  )}

                  {sec.codeSnippets && sec.codeSnippets.length > 0 && (
                    <div className="handbook-code-wrapper">
                      {sec.codeSnippets.map((snippet, idx) => {
                        const snippetKey = `${sec.id}-${idx}`;
                        const isCopied = copiedKey === snippetKey;
                        const isFailed = copyFailedKey === snippetKey;

                        return (
                          <div
                            key={idx}
                            className="terminal-code-block"
                            data-testid={`code-block-${snippetKey}`}
                          >
                            <div className="terminal-shell-bar">
                              <span className="shell-lang">
                                {snippet.caption || snippet.language || 'bash'}
                              </span>
                              <button
                                type="button"
                                className={`copy-code-btn ${isCopied ? 'copied' : ''} ${
                                  isFailed ? 'failed' : ''
                                }`}
                                onClick={() => handleCopy(snippetKey, snippet.code)}
                                aria-label={`Copy code for ${sec.title}`}
                                data-testid={`copy-btn-${snippetKey}`}
                              >
                                <span aria-hidden="true">
                                  {isCopied ? '✓' : isFailed ? '⚠️' : '📋'}
                                </span>
                                <span>
                                  {isCopied
                                    ? 'Copied to Clipboard!'
                                    : isFailed
                                      ? 'Copy failed, select manually'
                                      : 'Copy Code'}
                                </span>
                              </button>
                            </div>
                            <pre>
                              <code>{snippet.code}</code>
                            </pre>
                          </div>
                        );
                      })}
                    </div>
                  )}

                  {sec.callout && (
                    <div
                      className={`handbook-callout callout-${sec.callout.type}`}
                      data-testid={`callout-${sec.id}`}
                    >
                      <div className="callout-title">{sec.callout.title}</div>
                      <div className="callout-text">{sec.callout.text}</div>
                    </div>
                  )}
                </section>
              ))}
            </div>
          </div>

          {/* Chapter Navigation Buttons */}
          <nav className="chapter-nav-buttons" aria-label="Chapter pagination">
            {prevChapter ? (
              <a
                href={`#/docs/${prevChapter.id}`}
                className="btn-secondary"
                aria-label="Previous chapter"
                onClick={handleClearSearch}
                data-testid="prev-chapter-btn"
              >
                ← Ch. {prevChapter.number}: {prevChapter.title}
              </a>
            ) : (
              <div />
            )}
            {nextChapter ? (
              <a
                href={`#/docs/${nextChapter.id}`}
                className="btn-secondary"
                aria-label="Next chapter"
                onClick={handleClearSearch}
                data-testid="next-chapter-btn"
              >
                Ch. {nextChapter.number}: {nextChapter.title} →
              </a>
            ) : (
              <div />
            )}
          </nav>
        </article>
      </div>
    </section>
  );
}
