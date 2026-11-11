/**
 * The relationship explorer's pure logic: reading what /graph/entity and /graph/shortest-path
 * return, laying the result out, and saying it in words.
 *
 * Nothing here is invented. A link the graph did not store is never drawn, a path that was not
 * found is reported as not found, and a connection built from recent events rather than a stored,
 * rated link is marked as such (the backend sends no `source_type` for those).
 */

export interface RawConnection {
  source_name?: string | null;
  source_type?: string | null;
  relationship?: string | null;
  target_id?: string | null;
  target_name?: string | null;
  target_type?: string | null;
  weight?: number;
  confidence?: number;
  decayed_weight?: number;
  last_updated?: number;
  hop_level?: number;
}

export interface EntityGraphResponse {
  entity_id?: string;
  hops?: number;
  connections?: RawConnection[];
}

export interface GNode {
  id: string;
  name: string;
  type: string;
  hop: number;
}

export interface GEdge {
  from: string;
  to: string;
  relationship: string;
  strength: number;
  updated: number | null;
  /** True when the link was read from a stored, rated relationship. */
  stored: boolean;
}

export interface EntityView {
  center: string | null;
  nodes: GNode[];
  edges: GEdge[];
  /** Links built from recent events, not from a stored relationship. */
  fromEvents: boolean;
}

const clean = (s: string | null | undefined): string => (s ?? '').toString().trim();

/** `MORGAN_STANLEY` -> `Morgan Stanley`; a single word, or a name with lower-case letters, is left as it is. */
export function displayName(name: string): string {
  const s = clean(name);
  // One shouting word is a ticker or an acronym (VSAT, CACI, IBM) and stays; several are a name to tidy.
  if (/[a-z]/.test(s) || !/[A-Z]/.test(s) || !/[\s_]/.test(s)) return s;
  return s
    .replace(/_+/g, ' ')
    .toLowerCase()
    .replace(/(^|\s)([a-z])/g, (_m, space: string, letter: string) => `${space}${letter.toUpperCase()}`);
}

/** `InstitutionalFiler` -> `institutional filers`, `Country` -> `countries`. */
export function pluralKind(type: string): string {
  const words = clean(type).replace(/([a-z])([A-Z])/g, '$1 $2').toLowerCase() || 'entity';
  if (/[^aeiou]y$/.test(words)) return `${words.slice(0, -1)}ies`;
  return /(s|x|ch|sh)$/.test(words) ? `${words}es` : `${words}s`;
}

/** `CORRELATED_WITH` -> `correlated with`. */
export function relationshipWords(rel: string | null | undefined): string {
  const words = clean(rel).toLowerCase().replace(/_+/g, ' ').trim();
  return words || 'related to';
}

/** How firm a link is, in a word, from its age-adjusted strength (0 to 1 and a bit). */
export function strengthWords(strength: number): 'strong' | 'moderate' | 'faint' {
  if (strength >= 0.6) return 'strong';
  if (strength >= 0.25) return 'moderate';
  return 'faint';
}

export function parseEntityGraph(resp: EntityGraphResponse | null | undefined): EntityView {
  const rows = (resp?.connections ?? []).filter((c) => clean(c.source_name) && clean(c.target_name));
  if (rows.length === 0) return { center: null, nodes: [], edges: [], fromEvents: false };

  // The centre is the source of the first-hop links; with only deeper rows it is the most common first source.
  const first = rows.filter((c) => (c.hop_level ?? 1) <= 1);
  const center = clean((first[0] ?? rows[0]).source_name);
  const nodes = new Map<string, GNode>();
  const kind = new Map<string, string>();
  nodes.set(center, { id: center, name: center, type: clean(rows[0].source_type) || 'Entity', hop: 0 });

  const edges: GEdge[] = [];
  const seen = new Set<string>();
  for (const c of rows) {
    const from = clean(c.source_name);
    const to = clean(c.target_name);
    if (from === to) continue;
    const key = `${from}|${clean(c.relationship)}|${to}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const hop = Math.max(1, c.hop_level ?? 1);
    if (c.target_type) kind.set(to, clean(c.target_type));
    if (c.source_type) kind.set(from, clean(c.source_type));
    for (const name of [from, to]) {
      if (!nodes.has(name)) nodes.set(name, { id: name, name, type: 'Entity', hop });
    }
    const target = nodes.get(to)!;
    if (target.hop === 0 || hop < target.hop) target.hop = to === center ? 0 : hop;
    edges.push({
      from,
      to,
      relationship: relationshipWords(c.relationship),
      strength: c.decayed_weight ?? c.weight ?? 0,
      updated: c.last_updated && c.last_updated > 0 ? c.last_updated : null,
      stored: Boolean(c.source_type),
    });
  }
  for (const n of nodes.values()) n.type = kind.get(n.name) ?? n.type;
  const list = [...nodes.values()];
  return { center, nodes: list, edges, fromEvents: edges.every((e) => !e.stored) };
}

export interface Point {
  x: number;
  y: number;
}

/**
 * Rings around the centre: the entity in the middle, direct links on the first ring, links of links on the
 * second. Within a ring nodes are ordered by type then name, so same-kind neighbours sit together; a
 * second-ring node sits beside the first-ring node it hangs from. Deterministic, so a view does not
 * jump between renders.
 */
export function layoutRings(view: EntityView, size = 1): Map<string, Point> {
  const pos = new Map<string, Point>();
  if (!view.center) return pos;
  const c = size / 2;
  pos.set(view.center, { x: c, y: c });
  const ring1 = view.nodes.filter((n) => n.hop === 1).sort((a, b) => a.type.localeCompare(b.type) || a.name.localeCompare(b.name));
  const ring2 = view.nodes.filter((n) => n.hop >= 2);
  // A crowded ring is drawn smaller so each name has room outside its dot.
  const r1 = ring2.length ? size * 0.24 : ring1.length > 16 ? size * 0.3 : size * 0.36;
  const r2 = size * 0.44;
  const angle1 = new Map<string, number>();
  ring1.forEach((n, i) => {
    const a = (2 * Math.PI * i) / Math.max(1, ring1.length) - Math.PI / 2;
    angle1.set(n.name, a);
    pos.set(n.name, { x: c + r1 * Math.cos(a), y: c + r1 * Math.sin(a) });
  });
  const taken = new Map<string, number>();
  ring2
    .sort((a, b) => a.name.localeCompare(b.name))
    .forEach((n, i) => {
      const parent = view.edges.find((e) => (e.to === n.name && angle1.has(e.from)) || (e.from === n.name && angle1.has(e.to)));
      const parentName = parent ? (angle1.has(parent.from) ? parent.from : parent.to) : null;
      const base = parentName ? (angle1.get(parentName) as number) : (2 * Math.PI * i) / Math.max(1, ring2.length);
      const k = parentName ? (taken.get(parentName) ?? 0) : 0;
      if (parentName) taken.set(parentName, k + 1);
      const a = base + (k % 2 ? 1 : -1) * 0.12 * Math.ceil(k / 2 + (k === 0 ? 0 : 0.5));
      pos.set(n.name, { x: c + r2 * Math.cos(a), y: c + r2 * Math.sin(a) });
    });
  return pos;
}

export interface PathStep {
  name: string;
  /** What links this step to the next one; null on the last. */
  link: string | null;
}

export interface PathView {
  found: boolean;
  steps: PathStep[];
}

interface RawPathRecord {
  entities?: Array<Record<string, unknown>>;
  relations?: unknown[];
}

function nodeName(e: Record<string, unknown>): string {
  return clean(e.name as string) || clean(e.id as string) || clean(e.ticker as string);
}

function relType(r: unknown): string {
  if (Array.isArray(r)) return clean(r[1] as string);
  if (r && typeof r === 'object') return clean((r as { type?: string }).type);
  return clean(r as string);
}

/** The shortest route between two entities. An empty or malformed answer is "not found", never a guessed chain. */
export function parsePath(resp: { path?: RawPathRecord[] } | null | undefined): PathView {
  const rec = resp?.path?.[0];
  const names = (rec?.entities ?? []).map(nodeName).filter(Boolean);
  if (names.length < 2) return { found: false, steps: [] };
  const rels = rec?.relations ?? [];
  return {
    found: true,
    steps: names.map((name, i) => ({ name, link: i < names.length - 1 ? relationshipWords(relType(rels[i])) : null })),
  };
}

export interface Hub {
  id: string;
  name: string;
  type: string;
  degree: number;
}

/** Hubs grouped by kind, kinds with the most connections first, each group's own hubs by degree. */
export function groupHubs(hubs: readonly Hub[], perGroup = 8): Array<{ type: string; hubs: Hub[] }> {
  const groups = new Map<string, Hub[]>();
  for (const h of hubs) groups.set(h.type, [...(groups.get(h.type) ?? []), h]);
  return [...groups.entries()]
    .map(([type, list]) => ({ type, hubs: list.sort((a, b) => b.degree - a.degree).slice(0, perGroup) }))
    .sort((a, b) => b.hubs.reduce((s, h) => s + h.degree, 0) - a.hubs.reduce((s, h) => s + h.degree, 0));
}

/** Suggestions while typing: names and ids that start with, then contain, the text. */
export function matchHubs(hubs: readonly Hub[], query: string, limit = 7): Hub[] {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  const rank = (h: Hub): number | null => {
    const n = h.name.toLowerCase();
    const id = h.id.toLowerCase();
    if (n === q || id === q) return 0;
    if (n.startsWith(q) || id.startsWith(q)) return 1;
    return n.includes(q) || id.includes(q) ? 2 : null;
  };
  return hubs
    .map((h) => ({ h, r: rank(h) }))
    .filter((x): x is { h: Hub; r: number } => x.r !== null)
    .sort((a, b) => a.r - b.r || b.h.degree - a.h.degree)
    .slice(0, limit)
    .map((x) => x.h);
}
