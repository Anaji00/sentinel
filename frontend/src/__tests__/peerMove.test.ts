import { describe, expect, it } from 'vitest';
import { peerMove } from '../lib/peerMove';

describe('peerMove', () => {
  it('reads the move and the span', () => {
    expect(peerMove('CRWD moved -7.60% since the last five sessions while its peers were flat')).toEqual({ pct: -7.6, span: 'in 5 sessions' });
    expect(peerMove('ZS moved +11.91% since the previous close')).toEqual({ pct: 11.9, span: 'since the last close' });
  });
  it('returns null for anything it does not recognise', () => {
    expect(peerMove('ZS moved 3% since a long time ago')).toBeNull();
    expect(peerMove(null)).toBeNull();
    expect(peerMove('no move here')).toBeNull();
  });
});
