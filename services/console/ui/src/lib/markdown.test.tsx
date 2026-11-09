import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { InlineMarkdown, Markdown, parseInline, parseMarkdown, type MdInline, type MdNode } from './markdown';

const t = (text: string): MdInline => ({ type: 'text', text });
const code = (text: string): MdInline => ({ type: 'code', text });
const strong = (...children: MdInline[]): MdInline => ({ type: 'strong', children });
const em = (...children: MdInline[]): MdInline => ({ type: 'em', children });
const link = (href: string, ...children: MdInline[]): MdInline => ({ type: 'link', href, children });

describe('parseInline', () => {
  it.each<{ name: string; src: string; want: MdInline[] }>([
    { name: 'empty', src: '', want: [] },
    { name: 'plain text', src: 'just text', want: [t('just text')] },
    { name: 'inline code', src: 'run `go test` now', want: [t('run '), code('go test'), t(' now')] },
    { name: 'code is not formatted', src: '`**raw**`', want: [code('**raw**')] },
    { name: 'bold', src: 'Use **strict** mode', want: [t('Use '), strong(t('strict')), t(' mode')] },
    { name: 'underscore bold', src: 'a __b__ c', want: [t('a '), strong(t('b')), t(' c')] },
    { name: 'intraword underscores are literal', src: 'snake__case__name', want: [t('snake__case__name')] },
    { name: 'italic', src: 'be *clean* ok', want: [t('be '), em(t('clean')), t(' ok')] },
    { name: 'italic at line start', src: '*lead* in', want: [em(t('lead')), t(' in')] },
    { name: 'italic wraps bold', src: '*a **b** c*', want: [em(t('a '), strong(t('b')), t(' c'))] },
    { name: 'bold wraps underscore bold', src: '**a __b__**', want: [strong(t('a '), strong(t('b')))] },
    { name: 'crossed emphasis closes inner too', src: '__x *y__ z* w', want: [strong(t('x '), em(t('y'))), t(' z w')] },
    { name: 'emphasis wraps code', src: '**`x` y**', want: [strong(code('x'), t(' y'))] },
    {
      name: 'https link',
      src: 'see [Docs](https://github.com/azylman/aerial).',
      want: [t('see '), link('https://github.com/azylman/aerial', t('Docs')), t('.')],
    },
    { name: 'root-relative link', src: '[Console](/console/)', want: [link('/console/', t('Console'))] },
    { name: 'link label formatting', src: '[**bold** `c`](http://x.y)', want: [link('http://x.y', strong(t('bold')), t(' '), code('c'))] },
    { name: 'javascript link drops to label', src: 'and [Evil](javascript:alert(1)).', want: [t('and Evil).')] },
    { name: 'protocol-relative link drops to label', src: '[x](//evil.example)', want: [t('x')] },
    { name: 'quote in url drops to label', src: `[x](https://a'b)`, want: [t('x')] },
    { name: 'code inside url drops to label', src: '[x](https://`y`)', want: [t('x')] },
    { name: 'html stays text', src: '<img src=x onerror=alert(1)>', want: [t('<img src=x onerror=alert(1)>')] },
    { name: 'sentinel characters are stripped', src: 'a0b', want: [t('a0b')] },
    { name: 'stray closing marker text', src: '** not bold', want: [t('** not bold')] },
  ])('$name', ({ src, want }) => {
    expect(parseInline(src)).toEqual(want);
  });
});

describe('parseMarkdown', () => {
  it.each<{ name: string; src: string | null | undefined; want: MdNode[] }>([
    { name: 'null', src: null, want: [] },
    { name: 'undefined', src: undefined, want: [] },
    { name: 'empty', src: '', want: [] },
    { name: 'blank lines only', src: '\n  \n', want: [] },
    {
      name: 'paragraph soft breaks with CRLF',
      src: 'line one\r\nline *two*\r\n\r\nnext',
      want: [
        { type: 'paragraph', lines: [[t('line one')], [t('line '), em(t('two'))]] },
        { type: 'paragraph', lines: [[t('next')]] },
      ],
    },
    {
      name: 'headings h1 and h6',
      src: '# Top\n###### Deep `x`\n####### seven',
      want: [
        { type: 'heading', level: 1, children: [t('Top')] },
        { type: 'heading', level: 6, children: [t('Deep '), code('x')] },
        { type: 'paragraph', lines: [[t('####### seven')]] },
      ],
    },
    {
      name: 'horizontal rules',
      src: 'a\n---\n***\n___',
      want: [{ type: 'paragraph', lines: [[t('a')]] }, { type: 'hr' }, { type: 'hr' }, { type: 'hr' }],
    },
    {
      name: 'blockquote including empty quote',
      src: '> Quoted **invariant**\n>',
      want: [
        { type: 'blockquote', children: [t('Quoted '), strong(t('invariant'))] },
        { type: 'blockquote', children: [] },
      ],
    },
    {
      name: 'bullet markers share one list',
      src: '- one\n* two\n• three',
      want: [{ type: 'list', ordered: false, items: [[t('one')], [t('two')], [t('three')]] }],
    },
    {
      name: 'switching list kinds starts a new list',
      src: '- a\n1. b\n2. c\n- d',
      want: [
        { type: 'list', ordered: false, items: [[t('a')]] },
        { type: 'list', ordered: true, items: [[t('b')], [t('c')]] },
        { type: 'list', ordered: false, items: [[t('d')]] },
      ],
    },
    {
      name: 'paragraph line ends a list; list item ends a paragraph',
      src: '- a\ntext\n- b',
      want: [
        { type: 'list', ordered: false, items: [[t('a')]] },
        { type: 'paragraph', lines: [[t('text')]] },
        { type: 'list', ordered: false, items: [[t('b')]] },
      ],
    },
    {
      name: 'fenced code keeps raw lines and language',
      src: 'intro\n```go\nfunc main() {\n\n  # not a heading\n}\n```\nafter',
      want: [
        { type: 'paragraph', lines: [[t('intro')]] },
        { type: 'code', lang: 'go', text: 'func main() {\n\n  # not a heading\n}' },
        { type: 'paragraph', lines: [[t('after')]] },
      ],
    },
    {
      name: 'fence without language ends a list',
      src: '- a\n```\nx\n```',
      want: [
        { type: 'list', ordered: false, items: [[t('a')]] },
        { type: 'code', lang: '', text: 'x' },
      ],
    },
    { name: 'unterminated fence renders as code', src: '```sh\nls -la', want: [{ type: 'code', lang: 'sh', text: 'ls -la' }] },
  ])('$name', ({ src, want }) => {
    expect(parseMarkdown(src)).toEqual(want);
  });
});

// Rendered-markup expectations ported from the app.test.js renderMarkdown suite.
const SAMPLE = [
  '### Build Spec',
  'Use **strict** and *clean* `go test` with [Docs](https://github.com/azylman/aerial) and [Evil](javascript:alert(1)).',
  '',
  '- First bullet',
  '- Second `code` bullet',
  '',
  '1. Step one',
  '2. Step two',
  '',
  '> Quoted invariant',
  '',
  '---',
  'two',
  'lines',
  '',
  '```go',
  'func main() { fmt.Println("<ok>") }',
  '```',
].join('\n');

describe('Markdown', () => {
  it.each([
    { name: 'null source', source: null, want: '' },
    {
      name: 'full sample',
      source: SAMPLE,
      want:
        '<h3 class="md-heading md-h3">Build Spec</h3>' +
        '<p class="md-p">Use <strong>strict</strong> and <em>clean</em> <code class="md-inline-code">go test</code> with ' +
        '<a href="https://github.com/azylman/aerial" target="_blank" rel="noopener noreferrer" class="md-link">Docs</a> and Evil).</p>' +
        '<ul class="md-list"><li>First bullet</li><li>Second <code class="md-inline-code">code</code> bullet</li></ul>' +
        '<ol class="md-list md-ol"><li>Step one</li><li>Step two</li></ol>' +
        '<blockquote class="md-blockquote">Quoted invariant</blockquote>' +
        '<hr class="md-hr">' +
        '<p class="md-p">two<br>lines</p>' +
        '<pre class="md-code-block"><code data-lang="go">func main() { fmt.Println("&lt;ok&gt;") }</code></pre>',
    },
    { name: 'code without language has no data-lang', source: '```\n<b>\n```', want: '<pre class="md-code-block"><code>&lt;b&gt;</code></pre>' },
  ])('$name', ({ source, want }) => {
    const { container } = render(<Markdown source={source} />);
    expect(container.innerHTML).toBe(want);
  });
});

describe('InlineMarkdown', () => {
  it.each([
    { name: 'null source', source: null, want: '' },
    {
      name: 'acceptance criterion',
      source: 'Verify `node --test` passes **cleanly**',
      want: 'Verify <code class="md-inline-code">node --test</code> passes <strong>cleanly</strong>',
    },
    { name: 'html is escaped', source: '<script>x</script>', want: '&lt;script&gt;x&lt;/script&gt;' },
  ])('$name', ({ source, want }) => {
    const { container } = render(<InlineMarkdown source={source} />);
    expect(container.innerHTML).toBe(want);
  });
});
