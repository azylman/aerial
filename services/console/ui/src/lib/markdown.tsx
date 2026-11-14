// Minimal Markdown for build objectives, shared context and chunk instructions. The source is
// parsed into a small AST and rendered as React elements, so no HTML string is ever produced or
// parsed. Supported syntax is exactly this subset: ATX headings, blockquotes, horizontal rules,
// -/*/• and 1. lists, paragraphs (soft breaks become <br>), fenced code, `inline code`, **bold** /
// __bold__, *italic* and [links](...) restricted to http(s) and same-origin absolute paths.

import { Fragment, memo, type ReactNode } from 'react';

export type MdInline =
  | { readonly type: 'text'; readonly text: string }
  | { readonly type: 'code'; readonly text: string }
  | { readonly type: 'strong' | 'em'; readonly children: MdInline[] }
  | { readonly type: 'link'; readonly href: string; readonly children: MdInline[] };

export type HeadingLevel = 1 | 2 | 3 | 4 | 5 | 6;

export type MdNode =
  | { readonly type: 'heading'; readonly level: HeadingLevel; readonly children: MdInline[] }
  | { readonly type: 'blockquote'; readonly children: MdInline[] }
  | { readonly type: 'hr' }
  | { readonly type: 'list'; readonly ordered: boolean; readonly items: MdInline[][] }
  | { readonly type: 'paragraph'; readonly lines: MdInline[][] }
  | { readonly type: 'code'; readonly lang: string; readonly text: string };

// ---- inline parsing ----
//
// Stash inline code, stash links, then run the three emphasis regexes over the remaining string.
// The regexes emit private-use sentinel characters instead of tags, so emphasis can wrap stashed
// code/links (and nest); the sentinel stream is then folded into a tree.

const STASH_OPEN = '';
const STASH_CLOSE = '';
const STRONG_OPEN = '';
const STRONG_CLOSE = '';
const EM_OPEN = '';
const EM_CLOSE = '';
const SENTINELS = /[-]/g;
const TOKEN_SPLIT = /(\d+|[-])/;

const INLINE_CODE = /`([^`\n]+)`/g;
const LINK = /\[([^\]]+)\]\(([^)\s]+)\)/g;
const SAFE_URL = /^(?:https?:\/\/|\/(?!\/))[^\s"'-]*$/i;

function applyEmphasis(s: string): string {
  return s
    .replace(/\*\*([^*]+)\*\*/g, `${STRONG_OPEN}$1${STRONG_CLOSE}`)
    .replace(/(?<![a-zA-Z0-9])__([^_]+)__(?![a-zA-Z0-9])/g, `${STRONG_OPEN}$1${STRONG_CLOSE}`)
    .replace(/(^|[^*])\*([^*\n]+)\*(?!\*)/g, `$1${EM_OPEN}$2${EM_CLOSE}`);
}

type EmphasisNode = Extract<MdInline, { type: 'strong' | 'em' }>;

/** Folds a sentinel-marked string into inline nodes; crossed emphasis closes like an HTML parser. */
function buildInline(marked: string, stash: readonly MdInline[]): MdInline[] {
  const root: MdInline[] = [];
  const open: EmphasisNode[] = [];
  const current = (): MdInline[] => (open.length > 0 ? open[open.length - 1]!.children : root);
  const pushText = (text: string): void => {
    const into = current();
    const last = into.at(-1);
    if (last?.type === 'text') into[into.length - 1] = { type: 'text', text: last.text + text };
    else into.push({ type: 'text', text });
  };

  for (const part of marked.split(TOKEN_SPLIT)) {
    switch (part) {
      case '':
        break;
      case STRONG_OPEN:
      case EM_OPEN: {
        const node: EmphasisNode = { type: part === STRONG_OPEN ? 'strong' : 'em', children: [] };
        current().push(node);
        open.push(node);
        break;
      }
      case STRONG_CLOSE:
      case EM_CLOSE: {
        const type = part === STRONG_CLOSE ? 'strong' : 'em';
        const idx = open.findLastIndex((n) => n.type === type);
        if (idx >= 0) open.length = idx;
        break;
      }
      default:
        if (part.startsWith(STASH_OPEN)) current().push(stash[Number(part.slice(1, -1))]!);
        else pushText(part);
    }
  }
  return root;
}

/** Parses one line of inline Markdown. */
export function parseInline(src: string): MdInline[] {
  if (!src) return [];
  const stash: MdInline[] = [];
  const put = (node: MdInline): string => `${STASH_OPEN}${stash.push(node) - 1}${STASH_CLOSE}`;

  const withCode = src.replace(SENTINELS, '').replace(INLINE_CODE, (_, text: string) => put({ type: 'code', text }));
  const withLinks = withCode.replace(LINK, (_, label: string, href: string) =>
    SAFE_URL.test(href) ? put({ type: 'link', href, children: buildInline(applyEmphasis(label), stash) }) : label,
  );
  return buildInline(applyEmphasis(withLinks), stash);
}

// ---- block parsing ----

const FENCE = /^\s*```([a-zA-Z0-9_-]*)\s*$/;
const BLANK = /^\s*$/;
const HR = /^\s*(?:---|\*\*\*|___)\s*$/;
const HEADING = /^\s*(#{1,6})\s+(.+)$/;
const BLOCKQUOTE = /^\s*>\s?(.*)$/;
const BULLET = /^\s*[-*•]\s+(.+)$/;
const ORDERED = /^\s*\d+\.\s+(.+)$/;

/** Parses a Markdown document into block nodes. */
export function parseMarkdown(src: string | null | undefined): MdNode[] {
  if (!src) return [];
  const out: MdNode[] = [];
  let code: { lang: string; lines: string[] } | null = null;
  let list: { ordered: boolean; items: MdInline[][] } | null = null;
  let para: MdInline[][] = [];

  const flushPara = (): void => {
    if (para.length > 0) out.push({ type: 'paragraph', lines: para });
    para = [];
  };
  const flushList = (): void => {
    if (list) out.push({ type: 'list', ordered: list.ordered, items: list.items });
    list = null;
  };
  const flushBlocks = (): void => {
    flushPara();
    flushList();
  };
  const addListItem = (ordered: boolean, text: string): void => {
    flushPara();
    if (list?.ordered !== ordered) {
      flushList();
      list = { ordered, items: [] };
    }
    list.items.push(parseInline(text));
  };

  for (const line of src.replace(/\r\n/g, '\n').split('\n')) {
    const fence = FENCE.exec(line);
    if (fence) {
      if (code) {
        out.push({ type: 'code', lang: code.lang, text: code.lines.join('\n') });
        code = null;
      } else {
        flushBlocks();
        code = { lang: fence[1]!, lines: [] };
      }
      continue;
    }
    if (code) {
      code.lines.push(line);
      continue;
    }
    if (BLANK.test(line)) {
      flushBlocks();
      continue;
    }
    if (HR.test(line)) {
      flushBlocks();
      out.push({ type: 'hr' });
      continue;
    }
    const heading = HEADING.exec(line);
    if (heading) {
      flushBlocks();
      out.push({ type: 'heading', level: heading[1]!.length as HeadingLevel, children: parseInline(heading[2]!) });
      continue;
    }
    const quote = BLOCKQUOTE.exec(line);
    if (quote) {
      flushBlocks();
      out.push({ type: 'blockquote', children: parseInline(quote[1]!) });
      continue;
    }
    const bullet = BULLET.exec(line);
    if (bullet) {
      addListItem(false, bullet[1]!);
      continue;
    }
    const ordered = ORDERED.exec(line);
    if (ordered) {
      addListItem(true, ordered[1]!);
      continue;
    }
    flushList();
    para.push(parseInline(line));
  }
  // An unterminated fence still renders its collected lines as code.
  if (code) out.push({ type: 'code', lang: code.lang, text: code.lines.join('\n') });
  flushBlocks();
  return out;
}

// ---- rendering ----

function renderInline(nodes: readonly MdInline[]): ReactNode[] {
  return nodes.map((n, i) => {
    switch (n.type) {
      case 'text':
        return <Fragment key={i}>{n.text}</Fragment>;
      case 'code':
        return (
          <code key={i} className="md-inline-code">
            {n.text}
          </code>
        );
      case 'strong':
        return <strong key={i}>{renderInline(n.children)}</strong>;
      case 'em':
        return <em key={i}>{renderInline(n.children)}</em>;
      case 'link':
        return (
          <a key={i} href={n.href} target="_blank" rel="noopener noreferrer" className="md-link">
            {renderInline(n.children)}
          </a>
        );
    }
  });
}

function renderBlock(node: MdNode, key: number): ReactNode {
  switch (node.type) {
    case 'heading': {
      const H = `h${node.level}` as const;
      return (
        <H key={key} className={`md-heading md-h${node.level}`}>
          {renderInline(node.children)}
        </H>
      );
    }
    case 'blockquote':
      return (
        <blockquote key={key} className="md-blockquote">
          {renderInline(node.children)}
        </blockquote>
      );
    case 'hr':
      return <hr key={key} className="md-hr" />;
    case 'list': {
      const items = node.items.map((item, i) => <li key={i}>{renderInline(item)}</li>);
      return node.ordered ? (
        <ol key={key} className="md-list md-ol">
          {items}
        </ol>
      ) : (
        <ul key={key} className="md-list">
          {items}
        </ul>
      );
    }
    case 'paragraph':
      return (
        <p key={key} className="md-p">
          {node.lines.map((line, i) => (
            <Fragment key={i}>
              {i > 0 && <br />}
              {renderInline(line)}
            </Fragment>
          ))}
        </p>
      );
    case 'code':
      return (
        <pre key={key} className="md-code-block">
          <code {...(node.lang ? { 'data-lang': node.lang } : {})}>{node.text}</code>
        </pre>
      );
  }
}

export interface MarkdownProps {
  readonly source: string | null | undefined;
}

/** Block Markdown. Memoized on `source`, so unchanged poll data never re-parses. */
export const Markdown = memo(function Markdown({ source }: MarkdownProps) {
  return parseMarkdown(source).map(renderBlock);
});

/** Single-line Markdown (e.g. acceptance criteria list items). */
export const InlineMarkdown = memo(function InlineMarkdown({ source }: MarkdownProps) {
  return renderInline(parseInline(source ?? ''));
});
