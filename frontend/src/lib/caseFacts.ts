/**
 * The case file for a stock, grouped for reading: the facts the platform's agents share, each with its
 * id, where it came from and when it was true. Shown so a reader can check what a call rests on.
 */

export interface CaseFact {
  id: string;
  kind: string;
  text: string;
  value?: unknown;
  as_of?: string | null;
  source?: string | null;
}

export interface CaseFile {
  ticker?: string;
  built_at?: string;
  facts?: CaseFact[];
}

export interface FactGroup {
  kind: string;
  label: string;
  facts: CaseFact[];
}

const KIND_ORDER = ['rank', 'move', 'news', 'alert', 'feature', 'customer'] as const;

const KIND_LABEL: Record<string, string> = {
  rank: 'Model rank',
  move: 'Price moves',
  news: 'News',
  alert: 'Alerts',
  feature: 'Measured features',
  customer: 'Customers',
};

/** Where a fact came from, in words a reader can place. An unknown source is shown as it is stored. */
export const SOURCE_WORDS: Record<string, string> = {
  factor_model: 'the monthly factor model',
  daily_bars: 'daily prices',
  stock_news_llm: 'headlines scored by a language model',
  correlations: 'the alert engine',
  stock_features_daily: 'measured stock features',
  customer_links: "the company's latest 10-K",
};

export const sourceWords = (source: string | null | undefined): string =>
  source ? (SOURCE_WORDS[source] ?? source.replace(/_/g, ' ')) : 'an unrecorded source';

/** Facts grouped by kind in a fixed order, unknown kinds last, each group in file order. */
export function groupFacts(file: CaseFile | null | undefined): FactGroup[] {
  const groups = new Map<string, CaseFact[]>();
  for (const f of file?.facts ?? []) {
    if (!f || !f.id || !f.text) continue;
    groups.set(f.kind, [...(groups.get(f.kind) ?? []), f]);
  }
  const known = KIND_ORDER.filter((k) => groups.has(k));
  const other = [...groups.keys()].filter((k) => !(KIND_ORDER as readonly string[]).includes(k)).sort();
  return [...known, ...other].map((kind) => ({
    kind,
    label: KIND_LABEL[kind] ?? kind.replace(/_/g, ' '),
    facts: groups.get(kind) ?? [],
  }));
}
