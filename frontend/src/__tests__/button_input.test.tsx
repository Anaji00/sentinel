import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import React from 'react';
import { Button } from '../components/ui/Button';
import { Input } from '../components/ui/Input';
import { buttonClasses, inputClasses } from '../lib/ui';

describe('Button', () => {
  it('never submits a form by accident: type defaults to button, and can be set to submit', () => {
    expect(renderToStaticMarkup(<Button>Go</Button>)).toContain('type="button"');
    expect(renderToStaticMarkup(<Button type="submit">Go</Button>)).toContain('type="submit"');
  });

  it('says it is busy, and cannot be pressed while it is', () => {
    const html = renderToStaticMarkup(<Button loading>Save</Button>);
    expect(html).toContain('aria-busy="true"');
    expect(html).toContain('disabled=""');
    expect(renderToStaticMarkup(<Button>Save</Button>)).not.toContain('aria-busy');
  });

  it('announces a toggle', () => {
    expect(renderToStaticMarkup(<Button pressed>On</Button>)).toContain('aria-pressed="true"');
    expect(renderToStaticMarkup(<Button pressed={false}>Off</Button>)).toContain('aria-pressed="false"');
    expect(renderToStaticMarkup(<Button>Plain</Button>)).not.toContain('aria-pressed');
  });

  it('keeps a link a link: no padding, the surrounding text size', () => {
    const cls = buttonClasses('link', 'md');
    expect(cls).toContain('text-accent');
    expect(cls).not.toMatch(/\bpx-|\bpy-/);
    expect(buttonClasses('primary', 'md')).toContain('bg-accent');
  });

  it('uses only theme tokens, so both themes work', () => {
    for (const v of ['primary', 'secondary', 'outline', 'ghost', 'link', 'danger'] as const) {
      expect(buttonClasses(v, 'md')).not.toMatch(/#[0-9a-f]{3,6}|\b(?:text|bg|border)-(?:white|black|gray|slate|red|blue)/i);
    }
  });

  it('passes a caller class through last, for layout', () => {
    expect(buttonClasses('outline', 'sm', 'ml-auto')).toMatch(/ml-auto$/);
  });
});

describe('Input', () => {
  it('ties its label to the field', () => {
    const html = renderToStaticMarkup(<Input label="Ticker" id="t" />);
    expect(html).toContain('<label for="t"');
    expect(html).toContain('id="t"');
    expect(html).toContain('Ticker');
  });

  it('keeps the label for assistive tech when it is hidden', () => {
    expect(renderToStaticMarkup(<Input label="Search" hideLabel />)).toContain('sr-only');
  });

  it('links a hint to the field, and announces an error', () => {
    const hint = renderToStaticMarkup(<Input label="Ticker" id="t" hint="Letters only" />);
    expect(hint).toContain('aria-describedby="t-note"');
    expect(hint).not.toContain('role="alert"');
    const err = renderToStaticMarkup(<Input label="Ticker" id="t" error="Type a ticker" />);
    expect(err).toContain('aria-invalid="true"');
    expect(err).toContain('role="alert"');
    expect(err).toContain('border-caution');
  });

  it('gives each field its own id when none is passed', () => {
    const a = renderToStaticMarkup(<Input label="A" />);
    expect(a).toMatch(/for="([^"]+)"/);
    expect(inputClasses('md', false)).toContain('focus-visible:border-accent');
  });
});
