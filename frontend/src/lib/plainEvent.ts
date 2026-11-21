/**
 * Plain wording for the machine-written lines in the event feed. The collectors write for other programs: ship numbers,
 * distances in nautical miles, a model's "anomaly score", a betting contract's code. This rewrites the common shapes into
 * what a person would say, and leaves any line it does not recognise exactly as it came.
 */

const METRES_PER_NM = 1852;

function distance(nm: number): string {
  const m = nm * METRES_PER_NM;
  if (m < 1000) return `${Math.max(10, Math.round(m / 10) * 10)} metres`;
  return `${(m / 1000).toFixed(1)} kilometres`;
}

function hours(h: number): string {
  if (h < 1) return `${Math.max(1, Math.round(h * 60))} minutes`;
  return `${h.toFixed(1).replace(/\.0$/, '')} hours`;
}

/** A line of event text in plain words. */
export function plainEventText(text: string): string {
  if (!text) return text;
  let t = text;

  // "Possible STS: NAME alongside MMSI 123 for 1.6h at 0.204 nm"
  t = t.replace(
    /Possible STS:\s*(.+?) alongside MMSI \d+ for ([\d.]+)h at ([\d.]+) nm/gi,
    (_m, name: string, h: string, nm: string) => `${name} has sat ${distance(Number(nm))} from another ship for ${hours(Number(h))}, possibly passing cargo`,
  );
  // "MMSI 1 and MMSI 2 have been within 0.2 nm of each other, both under 0.0 knots, for 1.6 hours in Singapore Approach."
  t = t.replace(
    /MMSI \d+ and MMSI \d+ have been within ([\d.]+) nm of each other, both under ([\d.]+) knots?, for ([\d.]+) hours? in ([^.]+)\./gi,
    (_m, nm: string, kn: string, h: string, place: string) =>
      `Two ships have stayed within ${distance(Number(nm))} of each other for ${hours(Number(h))} in ${place.trim()}, ${Number(kn) <= 0.5 ? 'barely moving' : 'moving slowly'}.`,
  );
  t = t.replace(/That geometry is a ship-to-ship transfer or a rendezvous; it is not a berth, because neither hull is alongside a quay, and it is not traffic, because neither is making way\./g,
    'That looks like one ship passing cargo to another or two ships meeting. It is not a dock stop, and it is not ordinary traffic.');
  // "Ship position for Tanker 'X' (MMSI: 461000256) in Singapore Approach. Speed: 10.9 knots, Heading: 70°."
  t = t.replace(
    /Ship position for ([^(]+?)\s*\(MMSI:? \d+\)\s*in ([^.]+)\.\s*Speed: ([\d.]+) knots?, Heading: (\d+)°\./gi,
    (_m, what: string, place: string, kn: string) => `${what.trim()} in ${place.trim()}, ${Number(kn) < 0.5 ? 'not moving' : `moving at ${kn} knots`}.`,
  );
  t = t.replace(/ its MMSI or IMO is on the sanctions list/gi, ' the ship is on a sanctions list');
  t = t.replace(/\s*\(MMSI:? \d+\)/g, '');
  t = t.replace(/\bMMSI:? ?(\d+)\b/g, 'ship $1');

  // A betting contract: keep the question and the odds, drop the wrapper and the model's score.
  t = t.replace(/Prediction market contract '([^']+)' \(Market Odds\)\.\s*Current price\/probability: ([\d.]+)%\./g, (_m, q: string, p: string) => `${q} The market puts it at ${Math.round(Number(p))}%.`);
  t = t.replace(/\s*Anomaly score: [\d.]+\.?/g, '');
  return t.trim();
}

/** Drops the "Overall confidence 62%." sentence the model writes into a narrative: it goes stale when the status moves, and the stored figure is shown instead. */
export function withoutStaleConfidence(text: string | null | undefined): string {
  return (text ?? '').replace(/\s*Overall confidence(?: is| of)?:?\s*\d{1,3}(?:\.\d+)?\s*%\.?/gi, '').trim();
}

/** How unusual a reading is, in words, from its 0 to 1 score. */
export function unusualWords(score: number): string {
  if (score >= 0.9) return 'Very unusual';
  if (score >= 0.7) return 'Unusual';
  return 'Routine';
}

/** The source names the collectors use, as a person would name them. */
const SOURCES: Record<string, string> = {
  aisstream: 'ship tracking',
  kalshi: 'Kalshi betting market',
  polymarket: 'Polymarket betting market',
  finnhub: 'Finnhub',
  alpaca: 'Alpaca',
  gdelt: 'news monitoring',
  acled: 'conflict records',
};

/** The queue names behind the failed-events list, as what each one holds. The technical name stays in the hover. */
const TOPICS: Record<string, string> = {
  'enriched.events': 'Events being sorted and scored',
  'events.raw.news': 'News stories waiting to be read',
  'sentinel.ontology.proposals': 'Suggested links between companies',
  unknown: 'Source not recorded',
};

export function plainTopic(topic: string): string {
  return TOPICS[topic] ?? topic;
}

export function plainSource(source: string): string {
  return SOURCES[source.trim().toLowerCase()] ?? source;
}
