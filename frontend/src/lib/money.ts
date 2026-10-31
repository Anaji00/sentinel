/**
 * Follow the money: the shapes the /money page reads, and the pure layout it
 * draws with. The numbers come from 13F filings, 10-K customer disclosures,
 * USAspending and the federal lobbying register, through the gateway.
 */

export interface Concentration {
  companies: number;
  median_big_three_pct: number | null;
  median_tracked_pct?: number;
  notable: number;
  notable_threshold_pct?: number;
  top: { ticker: string; big_three_pct: number; tracked_pct: number }[];
}

export interface LargestHolder {
  filer_id: string;
  filer: string;
  companies: number;
  big_three: boolean;
}

export interface RivalPair {
  a: string;
  b: string;
  label: string;
  common_pct: number;
  shared_holders: number;
  big_three_a: number;
  big_three_b: number;
  /** Readable company names from SEC titles; the ticker when there is none. */
  a_name?: string;
  b_name?: string;
}

export interface CrossHolding {
  holder: string;
  holder_ticker: string | null;
  held: string;
  held_ticker: string | null;
  pct: number;
  reverse_pct: number | null;
}

export interface OwnershipOverview {
  concentration: Concentration;
  largest_holder: LargestHolder[];
  rivals: RivalPair[];
  mutual: CrossHolding[];
  note: string;
}

export interface SupplyGroup {
  buyer: string;
  buyer_name: string;
  suppliers: { ticker: string; name?: string; pct: number; filed: string }[];
}

export interface Holder {
  filer_id: string;
  filer: string;
  pct_outstanding: number | null;
  value_usd: number;
  period: string;
}

export interface HoldersResponse {
  ticker: string;
  /** Readable company name; the lookup also accepts a name ("walmart"). */
  name?: string | null;
  holders: Holder[];
  tracked_institutions_pct: number;
  big_three_pct: number;
}

export interface Interlock {
  person: string;
  companies: string[];
  roles: string[];
}

export const BIG_THREE_IDS = new Set(['BLACKROCK', 'VANGUARD', 'STATE_STREET']);

/** Short display names: "BlackRock, Inc." reads as "BlackRock" in a diagram. */
export function shortName(name: string): string {
  return name
    .replace(/,?\s+(Inc\.?|Corp\.?|Corporation|Group|Ltd\.?|LLC|& Co|Associates|Management|Advisors|Investment Management|Capital Management)$/i, '')
    .replace(/,?\s+(Inc\.?|Corp\.?|Group|Ltd\.?|LLC|& Co)$/i, '')
    .replace(/^FMR$/, 'Fidelity')
    .trim();
}

export interface NetNode {
  id: string;
  label: string;
  x: number;
  y: number;
  bigThree: boolean;
  /** Where the label sits: just outside the circle, anchored away from the centre. */
  labelX: number;
  labelY: number;
  anchor: 'start' | 'middle' | 'end';
}

export interface NetEdge {
  from: NetNode;
  to: NetNode;
  pct: number;
  mutual: boolean;
  width: number;
}

/**
 * Institutions on a circle, stakes as lines between them: the heaviest
 * `maxEdges` stakes, so the drawing stays readable. Width scales with the
 * stake, 1px to 6px.
 */
export function ownershipNetwork(
  rows: CrossHolding[],
  { size = 520, maxEdges = 28 }: { size?: number; maxEdges?: number } = {},
): { nodes: NetNode[]; edges: NetEdge[] } {
  const top = [...rows].filter((r) => r.pct > 0).sort((a, b) => b.pct - a.pct).slice(0, maxEdges);
  const names = Array.from(new Set(top.flatMap((r) => [r.holder, r.held])));
  // The index giants first, so they sit together at the top of the circle.
  names.sort((a, b) => Number(isBigThree(b)) - Number(isBigThree(a)));
  // Room outside the circle for the longest label ("Northern Trust", ~100px).
  const r = size / 2 - 120;
  const c = size / 2;
  const nodes = names.map((name, i) => {
    const angle = -Math.PI / 2 + (2 * Math.PI * i) / Math.max(1, names.length);
    const cos = Math.cos(angle);
    const sin = Math.sin(angle);
    // Labels sat above or below each node and ran into the lines on the sides
    // of the circle; outward along the radius they never cross an edge.
    return {
      id: name,
      label: shortName(name),
      x: Math.round(c + r * cos),
      y: Math.round(c + r * sin),
      bigThree: isBigThree(name),
      labelX: Math.round(c + (r + 14) * cos),
      labelY: Math.round(c + (r + 14) * sin + 4),
      anchor: cos > 0.3 ? 'start' : cos < -0.3 ? 'end' : 'middle',
    } as NetNode;
  });
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const maxPct = top.length ? top[0].pct : 1;
  const edges = top.map((row) => ({
    from: byId.get(row.holder)!,
    to: byId.get(row.held)!,
    pct: row.pct,
    mutual: row.reverse_pct != null,
    width: 1 + (5 * row.pct) / maxPct,
  }));
  return { nodes, edges };
}

function isBigThree(name: string): boolean {
  return /blackrock|vanguard|state street/i.test(name);
}

/** A share of 100 as a CSS width, clamped, for bars. */
export function pctWidth(pct: number | null | undefined, scale = 100): string {
  if (pct == null || !Number.isFinite(pct)) return '0%';
  return `${Math.max(0, Math.min(100, (pct / scale) * 100)).toFixed(1)}%`;
}
