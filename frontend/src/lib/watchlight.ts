/**
 * The pure parts of Watchlight (the Surveillance Watch page): the one-line verdict for a place, the council question drafted
 * from it, and the lines that show where a department's plate data is shared. Missing data is left out of the wording, and
 * "none on record" is never worded as "none".
 */

import { formatNumber } from './format';
import { STATE_CENTRES } from './usStates';

export interface VerdictCard {
  agency: string;
  nearby_devices?: Record<string, number>;
  technologies?: { technology: string }[];
  oversight?: unknown | null;
}

export interface VerdictPortal {
  shared_with?: unknown[];
  out_of_state_share?: number;
  shared_by_state?: Record<string, number>;
}

const num = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const count = (k: number, one: string, many: string): string => `${formatNumber(k, { decimals: 0 })} ${k === 1 ? one : many}`;

/** "Round Lake Park PD documents 3 kinds of surveillance. It shares plate data with 647 agencies, 540 in other states. No local oversight law is on record." */
export function verdictLine(card: VerdictCard, portal?: VerdictPortal | null): string {
  const parts: string[] = [];
  const kinds = card.technologies?.length ?? 0;
  const plates = num(card.nearby_devices?.ALPR);
  parts.push(kinds > 0 ? `${card.agency} documents ${count(kinds, 'kind', 'kinds')} of surveillance.` : `${card.agency} has no documented surveillance yet.`);
  if (plates != null && plates > 0) parts.push(`${count(plates, 'plate reader is', 'plate readers are')} mapped nearby.`);
  const shared = portal?.shared_with?.length;
  if (shared != null && shared > 0) {
    const out = num(portal?.out_of_state_share);
    parts.push(`It shares plate data with ${count(shared, 'agency', 'agencies')}${out != null && out > 0 ? `, ${formatNumber(out, { decimals: 0 })} in other states` : ''}.`);
  }
  parts.push(card.oversight ? 'A local oversight law applies.' : 'No local oversight law is on record.');
  return parts.join(' ');
}

/** The one claim to lead with: who can search the data, else what watches the area, else what is documented. */
export function verdictHeadline(card: VerdictCard, portal?: VerdictPortal | null): string {
  const shared = portal?.shared_with?.length ?? 0;
  if (shared > 0) return `${count(shared, 'agency can', 'agencies can')} search this department's plate data.`;
  const plates = num(card.nearby_devices?.ALPR);
  if (plates != null && plates > 0) return `${count(plates, 'plate reader watches', 'plate readers watch')} this area.`;
  const kinds = card.technologies?.length ?? 0;
  return kinds > 0 ? `${count(kinds, 'kind', 'kinds')} of surveillance on record here.` : 'No surveillance documented here yet.';
}

/** The question for a town council, drafted from the verdict. It is only drafted: the person sends it. */
export function councilQuestion(card: VerdictCard, portal?: VerdictPortal | null): string {
  const shared = portal?.shared_with?.length ?? 0;
  const asks = [
    'Who can search our license-plate reader data, and how long is it kept?',
    shared > 0
      ? `Public records show this department shares plate data with ${shared} agencies. Which of them, and under what written agreement?`
      : 'Does the department share plate data with other agencies, and under what written agreement?',
    'Is sharing with out-of-state or federal agencies allowed, and is that written down anywhere?',
    card.oversight ? 'How is the local oversight law applied to these systems?' : 'Is there a policy, a public audit log, or an oversight process for these systems?',
  ];
  return [`Hello. I live in your community and I am writing about ${card.agency}'s surveillance technology.`, '', ...asks.map((a, i) => `${i + 1}. ${a}`), '', 'Please answer in writing or put this on a meeting agenda. Thank you.'].join('\n');
}

export interface TrailLine {
  state: string;
  count: number;
  to: [number, number];
}

/** One line per state the data is shared with, biggest first. Federal and unknown places have no point to draw to. */
export function trailLines(sharedByState: Record<string, number> | null | undefined): TrailLine[] {
  return Object.entries(sharedByState ?? {})
    .filter(([st, c]) => STATE_CENTRES[st] && c > 0)
    .map(([state, c]) => ({ state, count: c, to: STATE_CENTRES[state] }))
    .sort((a, b) => b.count - a.count);
}

/** A gentle arc between two points, as map coordinates, so lines read as flows and do not pile up straight. */
export function arc(from: [number, number], to: [number, number], steps = 20): [number, number][] {
  const [la1, lo1] = from;
  const [la2, lo2] = to;
  const dx = lo2 - lo1;
  const dy = la2 - la1;
  const len = Math.hypot(dx, dy) || 1;
  const lift = len * 0.18;
  const nx = -dy / len;
  const ny = dx / len;
  return Array.from({ length: steps + 1 }, (_, i) => {
    const t = i / steps;
    const bend = Math.sin(Math.PI * t) * lift;
    return [la1 + dy * t + nx * bend, lo1 + dx * t + ny * bend] as [number, number];
  });
}
