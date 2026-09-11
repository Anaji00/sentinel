import { NormalizedEvent } from './types';

/**
 * Which domain an event belongs to, decided the way the gateway decides it.
 *
 * There were two answers to this question in the client and they disagreed.
 * `domainMetaFor()` reads the authoritative `domain` the gateway now returns and
 * falls back to the payload column -- that is the repair the
 * `event_domain_attribution` suite pins. `useLiveEvents` filtered the WebSocket
 * stream with `type.includes('market') || type.includes('price') || ...`, which
 * is the guess the gateway change existed to replace. Live consequence:
 * `prediction_market_trade` contains "market", so every Polymarket row was
 * badged PREDICTION by one function and listed under the TRADFI tab by the
 * other, in the same view.
 *
 * Both now call this. It returns `null` when the row carries nothing to decide
 * on -- neither a declared domain nor a payload -- rather than guessing, so the
 * caller can choose what to do with a row the server did not classify.
 */
export const KNOWN_DOMAINS = [
  'crypto',
  'prediction',
  'maritime',
  'aviation',
  'cyber',
  'tradfi',
  'news',
] as const;

export type EventDomain = (typeof KNOWN_DOMAINS)[number];

function isKnown(d: string | undefined): d is EventDomain {
  return Boolean(d) && (KNOWN_DOMAINS as readonly string[]).includes(d as string);
}

export function resolveEventDomain(e: NormalizedEvent): EventDomain | null {
  // What the server said. It decides by which payload column the row actually
  // carries, which is the only authoritative answer.
  const declared = (e as { domain?: string }).domain;
  if (isKnown(declared)) return declared;

  // The payload itself, for rows that predate the field or arrive by a path
  // that does not set it. Crypto first: a row carrying both a crypto and a
  // financial payload is a crypto row with an equity-shaped enrichment.
  if (e.crypto_data) return 'crypto';
  if (e.prediction_market_data) return 'prediction';
  if (e.vessel_data) return 'maritime';
  if (e.flight_data) return 'aviation';
  if (e.security_data) return 'cyber';
  if (e.financial_data) return 'tradfi';

  return null;
}
