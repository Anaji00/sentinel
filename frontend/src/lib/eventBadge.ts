/**
 * What a feed row may claim about an event.
 *
 * A company-news event has no measured anomaly: its score is a fixed placeholder
 * (EVENT_ANOMALY_SCORE in shared/utils/stock_news.py). Before that it was the size
 * of FinBERT's tone, so every one read CRITICAL, an employer award included. What
 * we do know is how positive or negative the wording is, so that is what it shows.
 */

const COMPANY_NEWS_SOURCE = 'alpaca_news';

export interface BadgeEvent {
  source?: string;
  financial_data?: object;
  domain_data?: Record<string, unknown>;
  primary_entity?: { name?: string };
  primary_entity_id?: string;
  primary_entity_name?: string;
  entity_name?: string;
}

export interface NewsTone {
  label: 'Positive tone' | 'Negative tone' | 'Company news';
  tone: number | null;
}

/** The tone reading for a company-news event; null for every other event. */
export function companyNewsTone(e: BadgeEvent): NewsTone | null {
  if (e.source !== COMPANY_NEWS_SOURCE) return null;
  // The live feed and the REST branches carry the payload under different names.
  const raw = (e.financial_data as { tone?: unknown } | undefined)?.tone ?? e.domain_data?.tone;
  const tone = typeof raw === 'number' && Number.isFinite(raw) ? raw : null;
  if (tone === null || tone === 0) return { label: 'Company news', tone };
  return { label: tone > 0 ? 'Positive tone' : 'Negative tone', tone };
}

/** Who the event is about, falling back to the ticker. Empty when nothing says. */
export function entityLabel(e: BadgeEvent): string {
  return e.primary_entity_name || e.entity_name || e.primary_entity?.name || e.primary_entity_id || '';
}
