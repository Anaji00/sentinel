import { describe, expect, it } from 'vitest';
import { canAsk, PENDING_POLL_MS, thesisKey, thesisRefresh, type ThesisState } from '../lib/thesis';

const state = (status: ThesisState['status']): ThesisState => ({
  ticker: 'NVDA', status, message: '', generated_at: null, model: null, bull: [], bear: [],
  would_change_it: '', facts_used: 0, disclaimer: '',
});

describe('thesis', () => {
  it('builds the route from a ticker, encoded', () => {
    expect(thesisKey('NVDA')).toBe('/stocks/NVDA/thesis');
    expect(thesisKey('A/B')).toBe('/stocks/A%2FB/thesis');
  });

  it('polls only while it is being written', () => {
    expect(thesisRefresh(state('pending'))).toBe(PENDING_POLL_MS);
    for (const s of ['none', 'ready', 'failed', 'busy', 'limited'] as const) expect(thesisRefresh(state(s))).toBe(0);
    expect(thesisRefresh(undefined)).toBe(0);
  });

  it('offers the button when nothing is there or the last try did not work, never while running or ready', () => {
    expect(canAsk(undefined)).toBe(true);
    expect(canAsk(state('none'))).toBe(true);
    expect(canAsk(state('failed'))).toBe(true);
    expect(canAsk(state('busy'))).toBe(true);
    expect(canAsk(state('pending'))).toBe(false);
    expect(canAsk(state('ready'))).toBe(false);
    expect(canAsk(state('limited'))).toBe(false);
  });
});
