/**
 * The move behind a "stocks moving against their peers" row, pulled from the alert's description so it can lead the row as a
 * figure: "moved -7.60% since the last five sessions" gives -7.6 and "in 5 sessions". Null when the description has no such
 * phrase, so the row falls back to the original text.
 */

export interface PeerMove {
  pct: number;
  span: string;
}

const SPANS: [RegExp, string][] = [
  [/since the last five sessions/i, 'in 5 sessions'],
  [/since the previous close/i, 'since the last close'],
];

export function peerMove(description: string | null | undefined): PeerMove | null {
  const m = /moved\s+([+-]?\d+(?:\.\d+)?)%\s+(since [^:]*?)(?=\s+while\b|:|$)/i.exec(description ?? '');
  if (!m) return null;
  const pct = Number(m[1]);
  if (!Number.isFinite(pct)) return null;
  const span = SPANS.find(([re]) => re.test(m[2]))?.[1];
  return span ? { pct: Math.round(pct * 10) / 10, span } : null;
}
