import { describe, expect, it } from 'vitest';
import { agentName, agentTier, pairLabel, scopeLabel, vesselTypeLabel } from '../lib/agentNames';

describe('agent names', () => {
  it('gives known agents a plain name', () => {
    expect(agentName('macro_intelligence_engine')).toBe('Economy and rates');
  });

  it('still shows an agent nobody has named yet', () => {
    expect(agentName('new_thing_agent')).toBe('new thing agent');
    expect(agentName(null)).toBe('');
  });

  it('reads tiers in any case and keeps an unknown one', () => {
    expect(agentTier('HEAVY')).toBe('Thorough');
    expect(agentTier('fast')).toBe('Quick');
    expect(agentTier('other')).toBe('other');
  });

  it('reads an event pair and a scope as words', () => {
    expect(pairLabel('news->tradfi')).toBe('News → Markets');
    expect(pairLabel('aviation->other')).toBe('Flights → other');
    expect(scopeLabel('momentum_long / bull')).toBe('momentum long, bull');
    expect(scopeLabel('')).toBe('overall');
  });

  it('reads a ship type as words', () => {
    expect(vesselTypeLabel('CargoVessel')).toBe('Cargo ship');
    expect(vesselTypeLabel('Unknown(16)')).toBe('Ship');
    expect(vesselTypeLabel('Tanker')).toBe('Tanker');
  });
});
