import { describe, it, expect } from 'vitest';
import { ownershipNetwork, pctWidth, shortName, type CrossHolding } from '../lib/money';

const row = (holder: string, held: string, pct: number, reverse_pct: number | null = null): CrossHolding => ({
  holder, held, pct, reverse_pct, holder_ticker: null, held_ticker: null,
});

describe('follow the money layout', () => {
  it('shortens filer names for a diagram', () => {
    expect(shortName('BlackRock, Inc.')).toBe('BlackRock');
    expect(shortName('Vanguard Group')).toBe('Vanguard');
    expect(shortName('Goldman Sachs Group')).toBe('Goldman Sachs');
    expect(shortName('FMR')).toBe('Fidelity');
  });

  it('keeps the heaviest stakes, puts the index giants first, and scales lines', () => {
    const rows = [
      row('Goldman Sachs Group', 'Morgan Stanley', 1),
      row('Vanguard Group', 'State Street Corp', 13.2),
      row('BlackRock, Inc.', 'State Street Corp', 8.8, 3.9),
      row('JPMorgan Chase & Co', 'Bank of America Corp', 0),
    ];
    const { nodes, edges } = ownershipNetwork(rows, { size: 400, maxEdges: 2 });
    expect(edges).toHaveLength(2);
    expect(edges[0].pct).toBe(13.2);
    expect(edges[0].width).toBe(6);
    expect(edges[1].mutual).toBe(true);
    expect(nodes.map((n) => n.bigThree)).toEqual([true, true, true]);
    // On the circle and inside the drawing.
    for (const n of nodes) {
      expect(n.x).toBeGreaterThanOrEqual(0);
      expect(n.x).toBeLessThanOrEqual(400);
    }
  });

  it('puts labels outside the circle, anchored away from the centre', () => {
    const rows = [row('A', 'B', 3), row('C', 'D', 2)];
    const { nodes } = ownershipNetwork(rows, { size: 400 });
    for (const n of nodes) {
      const inner = Math.hypot(n.x - 200, n.y - 200);
      const outer = Math.hypot(n.labelX - 200, n.labelY - 4 - 200);
      expect(outer).toBeGreaterThan(inner);
      if (n.x > 260) expect(n.anchor).toBe('start');
      if (n.x < 140) expect(n.anchor).toBe('end');
    }
  });

  it('draws nothing from nothing', () => {
    expect(ownershipNetwork([])).toEqual({ nodes: [], edges: [] });
  });

  it('clamps bar widths', () => {
    expect(pctWidth(50)).toBe('50.0%');
    expect(pctWidth(140)).toBe('100.0%');
    expect(pctWidth(-3)).toBe('0.0%');
    expect(pctWidth(null)).toBe('0%');
    expect(pctWidth(10, 20)).toBe('50.0%');
  });
});
