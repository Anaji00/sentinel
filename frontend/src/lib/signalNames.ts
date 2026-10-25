/**
 * Short, plain names for the signal families (shared/utils/signal_families.py).
 * The raw key stays available as a tooltip for anyone cross-checking the study.
 */
export const FAMILY_NAME: Record<string, string> = {
  earnings_reaction_up: 'Strong earnings-day jump',
  earnings_reaction_down: 'Sharp earnings-day drop',
  earnings_surprise_positive: 'Earnings beat',
  earnings_surprise_negative: 'Earnings miss',
  zone_escalation: 'Conflict escalation',
  zone_deescalation: 'Ceasefire',
  sector_news_positive: 'Good sector news',
  sector_news_negative: 'Bad sector news',
  country_news_positive: 'Good country news',
  country_news_negative: 'Bad country news',
  company_news_positive: 'Good company news',
  company_news_negative: 'Bad company news',
  dark_vessel_in_zone: 'Ship went dark in a conflict zone',
  tanker_sts_in_zone: 'Tanker transfer in a conflict zone',
  radar_no_ais: 'Ships not broadcasting',
  radar_traffic_drop: 'Shipping traffic drop',
  activist_13d: 'Activist investor stake',
  insider_cluster: 'Several insiders buying',
  contract_award: 'Federal contract win',
  options_call_sweep: 'Big call-option bet',
  options_put_sweep: 'Big put-option bet',
  volume_spike: 'Volume spike',
  block_trade: 'Large block trade',
  block_buy: 'Large block purchase',
  block_sell: 'Large block sale',
  institutional_accumulation: 'Funds piling in',
  institutional_distribution: 'Funds heading out',
  '8k_nonreliance': 'Past financials withdrawn',
  '8k_delisting_notice': 'Delisting notice',
  '8k_bankruptcy': 'Bankruptcy filing',
  '8k_impairment': 'Write-down',
  '8k_auditor_change': 'Auditor change',
  '8k_dilution': 'New shares sold privately',
  '8k_officer_change': 'Executive change',
  '8k_material_agreement': 'Major new agreement',
  '8k_acquisition_completed': 'Deal completed',
  short_volume_spike: 'Short-selling spike',
};

/** A family's plain name, or its key made readable when it is new. */
export function familyName(key: string): string {
  if (FAMILY_NAME[key]) return FAMILY_NAME[key];
  const words = key.replace(/^8k_/, 'filing ').replace(/_/g, ' ');
  return words.charAt(0).toUpperCase() + words.slice(1);
}

/** "5d" -> "next 5 days", "1d" -> "next day", "3h" -> "next 3 hours". */
export function horizonWords(h: string): string {
  const m = /^(\d+)\s*([dhw])$/i.exec(h.trim());
  if (!m) return h;
  const n = Number(m[1]);
  const unit = { d: 'day', h: 'hour', w: 'week' }[m[2].toLowerCase() as 'd' | 'h' | 'w'];
  return n === 1 ? `next ${unit}` : `next ${n} ${unit}s`;
}
