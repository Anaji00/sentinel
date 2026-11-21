/**
 * Groups the Surveillance Watch feed so one finding repeated for dozens of departments reads as one story ("Out-of-state
 * sharing needs signed declarations: 18 departments") instead of 18 near-identical lines. Only items of the same kind with the
 * same lead (the title before " — ") and at least three places are grouped; everything else stays as it is.
 */

export interface FeedLike {
  kind: string;
  title: string;
  ref?: string | null;
  url?: string | null;
  at: string | null;
}

export type FeedEntry<T extends FeedLike> = { type: 'single'; item: T } | { type: 'group'; kind: string; lead: string; items: T[]; places: string[] };

const SPLIT = ' — ';

export function splitTitle(title: string): { lead: string; place: string | null } {
  const i = title.lastIndexOf(SPLIT);
  return i > 0 ? { lead: title.slice(0, i), place: title.slice(i + SPLIT.length) } : { lead: title, place: null };
}

export function groupFeed<T extends FeedLike>(items: T[], minGroup = 3): FeedEntry<T>[] {
  const buckets = new Map<string, T[]>();
  for (const it of items) {
    const { lead, place } = splitTitle(it.title);
    if (place == null) continue;
    const key = `${it.kind}|${lead}`;
    buckets.set(key, [...(buckets.get(key) ?? []), it]);
  }
  const out: FeedEntry<T>[] = [];
  const done = new Set<string>();
  for (const it of items) {
    const { lead, place } = splitTitle(it.title);
    const key = `${it.kind}|${lead}`;
    const bucket = place != null ? buckets.get(key) : undefined;
    if (!bucket || bucket.length < minGroup) {
      out.push({ type: 'single', item: it });
      continue;
    }
    if (done.has(key)) continue;
    done.add(key);
    out.push({ type: 'group', kind: it.kind, lead, items: bucket, places: bucket.map((b) => splitTitle(b.title).place ?? b.title) });
  }
  return out;
}
