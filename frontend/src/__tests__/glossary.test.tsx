import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { GLOSSARY, defineTerm } from '../lib/glossary';
import { Term } from '../components/ui/Term';

describe('glossary', () => {
  it('defines terms case-insensitively and says nothing for an unknown one', () => {
    expect(defineTerm('Sharpe')).toBe(GLOSSARY.sharpe);
    expect(defineTerm('  DEFLATED SHARPE ')).toContain('0.95');
    expect(defineTerm('not a term')).toBeNull();
    expect(defineTerm('')).toBeNull();
  });

  it('keeps every definition short and free of the terms it is explaining away', () => {
    for (const [word, text] of Object.entries(GLOSSARY)) {
      expect(text.length, word).toBeLessThan(180);
      expect(text.length, word).toBeGreaterThan(30);
    }
  });
});

describe('Term', () => {
  it('links the meaning to the word for assistive technology', () => {
    const html = renderToStaticMarkup(<Term word="sharpe">Net Sharpe</Term>);
    expect(html).toContain('role="tooltip"');
    expect(html).toMatch(/aria-describedby="([^"]+)"/);
    const id = /aria-describedby="([^"]+)"/.exec(html)![1];
    expect(html).toContain(`id="${id}"`);
    expect(html).toContain('tabindex="0"');
  });

  it('draws the text plain when there is no definition', () => {
    const html = renderToStaticMarkup(<Term word="nonsense">Plain</Term>);
    expect(html).toBe('Plain');
    expect(renderToStaticMarkup(<Term word="">Plain</Term>)).toBe('Plain');
  });
});
