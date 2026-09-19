'use client';

/**
 * The subset of Markdown the report generator actually emits.
 *
 * Measured against a generated brief rather than guessed at: one h1, five h2,
 * sixteen list items, six rules, twenty-two table rows, eleven paragraphs,
 * sixty-six bold spans and a hundred and twenty code spans. No images, no
 * links, no nesting, no blockquotes, no fenced blocks.
 *
 * So this renders that and nothing else. A Markdown library would be a
 * dependency for a document shape the platform controls at both ends, and
 * CLAUDE.md is explicit that a package is not added when what exists will do.
 *
 * Nothing here interprets HTML. Every value goes through React as text, so a
 * report whose content happens to contain angle brackets renders them rather
 * than executing them -- which matters because report sections are built from
 * event headlines, and those come from feeds.
 */

import React from 'react';

/** `**bold**` and `` `code` ``, the only inline forms the generator produces. */
function inline(text: string, keyPrefix: string): React.ReactNode[] {
  const out: React.ReactNode[] = [];
  // One pass, alternating on the two delimiters. Deliberately not a regex
  // with backtracking: report lines can be long and a catastrophic pattern
  // here would hang the render.
  const pattern = /(\*\*[^*]+\*\*|`[^`]+`)/g;
  let last = 0;
  let m: RegExpExecArray | null;
  let i = 0;
  while ((m = pattern.exec(text)) !== null) {
    if (m.index > last) out.push(text.slice(last, m.index));
    const token = m[0];
    if (token.startsWith('**')) {
      out.push(
        <strong key={`${keyPrefix}-b${i}`} className="font-semibold text-ink">
          {token.slice(2, -2)}
        </strong>,
      );
    } else {
      out.push(
        <code
          key={`${keyPrefix}-c${i}`}
          className="rounded bg-inset px-1 py-0.5 font-mono text-micro text-accent"
        >
          {token.slice(1, -1)}
        </code>,
      );
    }
    last = m.index + token.length;
    i += 1;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}

/** `| a | b |` into cells, dropping the leading and trailing pipes. */
function cells(row: string): string[] {
  return row
    .trim()
    .replace(/^\||\|$/g, '')
    .split('|')
    .map((c) => c.trim());
}

const SEPARATOR = /^\|?[\s:|-]+\|?$/;

export function Markdown({ source }: { source: string }) {
  const blocks: React.ReactNode[] = [];
  const lines = source.split('\n');
  let i = 0;
  let key = 0;

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    if (!trimmed) {
      i += 1;
      continue;
    }

    if (trimmed.startsWith('---')) {
      blocks.push(<hr key={key++} className="my-3 border-line" />);
      i += 1;
      continue;
    }

    if (trimmed.startsWith('### ')) {
      blocks.push(
        <h4 key={key++} className="mt-3 text-xs font-semibold text-ink">
          {inline(trimmed.slice(4), `h4-${key}`)}
        </h4>,
      );
      i += 1;
      continue;
    }
    if (trimmed.startsWith('## ')) {
      blocks.push(
        <h3 key={key++} className="mt-4 text-sm font-semibold text-ink">
          {inline(trimmed.slice(3), `h3-${key}`)}
        </h3>,
      );
      i += 1;
      continue;
    }
    if (trimmed.startsWith('# ')) {
      blocks.push(
        <h2 key={key++} className="text-head font-semibold text-ink">
          {inline(trimmed.slice(2), `h2-${key}`)}
        </h2>,
      );
      i += 1;
      continue;
    }

    // A table runs until the first line that is not a row.
    if (trimmed.startsWith('|')) {
      const rows: string[] = [];
      while (i < lines.length && lines[i].trim().startsWith('|')) {
        rows.push(lines[i]);
        i += 1;
      }
      const body = rows.filter((r) => !SEPARATOR.test(r.trim()));
      if (body.length === 0) continue;
      const [head, ...rest] = body;
      blocks.push(
        // Its own scroll container: a wide table must not make the page
        // scroll sideways.
        <div key={key++} className="my-2 overflow-x-auto">
          <table className="w-full text-left text-micro">
            <thead>
              <tr className="border-b border-line">
                {cells(head).map((c, ci) => (
                  <th key={ci} className="stat-label whitespace-nowrap px-2 py-1">
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rest.map((r, ri) => (
                <tr key={ri} className="border-b border-line/60">
                  {cells(r).map((c, ci) => (
                    <td key={ci} className="px-2 py-1 text-ink-dim">
                      {inline(c, `t${ri}-${ci}`)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>,
      );
      continue;
    }

    if (/^[-*]\s/.test(trimmed)) {
      const items: string[] = [];
      while (i < lines.length && /^[-*]\s/.test(lines[i].trim())) {
        items.push(lines[i].trim().slice(2));
        i += 1;
      }
      blocks.push(
        <ul key={key++} className="my-1.5 space-y-0.5 pl-4">
          {items.map((it, ii) => (
            <li key={ii} className="list-disc text-xs leading-relaxed text-ink-dim">
              {inline(it, `li${key}-${ii}`)}
            </li>
          ))}
        </ul>,
      );
      continue;
    }

    blocks.push(
      <p key={key++} className="my-1.5 text-xs leading-relaxed text-ink-dim">
        {inline(trimmed, `p-${key}`)}
      </p>,
    );
    i += 1;
  }

  return <div className="space-y-0.5">{blocks}</div>;
}

export default Markdown;
