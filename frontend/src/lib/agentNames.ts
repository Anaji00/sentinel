/**
 * Plain names for the AI analysts (services/agents/). The raw key stays available
 * as a tooltip for anyone reading the logs; an agent not listed here shows its key
 * with spaces, so a new one still reads rather than disappearing.
 */
const AGENT_NAME: Record<string, string> = {
  analyst_agent: 'Stock alert analyst',
  analyst_shadow: 'Stock alert analyst (trial model)',
  consensus_engine: 'Agreement checker',
  edge_validator: 'Company link tester',
  macro_intelligence_engine: 'Economy and rates',
  quant_trading_engine: 'Trade ideas',
  radar_agent: 'Unusual activity radar',
  rule_synthesizer: 'Alert rule writer',
  stock_correlation_agent: 'Stocks that move together',
  supervisor: 'Knowledge graph gatekeeper',
};

const DOMAIN_NAME: Record<string, string> = {
  aviation: 'Flights',
  crypto: 'Crypto',
  cyber: 'Cyber',
  maritime: 'Ships',
  news: 'News',
  prediction: 'Prediction markets',
  tradfi: 'Markets',
};

const AGENT_TIER: Record<string, string> = {
  heavy: 'Thorough',
  fast: 'Quick',
};

const VESSEL_TYPE: Record<string, string> = {
  CargoVessel: 'Cargo ship',
  Fishing: 'Fishing vessel',
  Military: 'Military vessel',
  Passenger: 'Passenger ship',
  SpecialCraft: 'Special craft',
  WIG: 'Wing-in-ground craft',
};

/** A stored vessel category as words (vessel_type_label in shared/utils/regions.py). */
export function vesselTypeLabel(type: string): string {
  return type.startsWith('Unknown') ? 'Ship' : (VESSEL_TYPE[type] ?? type);
}

export function agentName(key: string | null | undefined): string {
  if (!key) return '';
  return AGENT_NAME[key] ?? key.replace(/_/g, ' ');
}

/** "news->tradfi" as "News → Markets". */
export function pairLabel(pair: string): string {
  return pair
    .split('->')
    .map((d) => DOMAIN_NAME[d.trim()] ?? d.trim())
    .join(' → ');
}

/** What a track record covers: "momentum_long / bull" reads as "momentum long, bull". */
export function scopeLabel(scope: string | null | undefined): string {
  if (!scope) return 'overall';
  return scope.replace(/_/g, ' ').replace(' / ', ', ');
}

export function agentTier(tier: string | null | undefined): string {
  if (!tier) return '';
  return AGENT_TIER[tier.toLowerCase()] ?? tier;
}
