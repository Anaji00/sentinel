import { describe, it, expect } from 'vitest';
import { actionWords, compactIds } from '../components/AuditTrail';

describe('audit trail in plain words', () => {
  it('turns action codes into words', () => {
    expect(actionWords('EXECUTE_BUY_ORDER')).toBe('Execute buy order');
    expect(actionWords('REPLAY_DEAD_LETTERS')).toBe('Replay dead letters');
    expect(actionWords('ORDER')).toBe('Order');
  });

  it('summarises long id lists and leaves short ones alone', () => {
    expect(compactIds('11505,11506,11507,11510')).toBe('4 items (11505–11510)');
    expect(compactIds('a1, b2, c3, d4, e5')).toBe('a1, b2 and 3 more');
    expect(compactIds('99311ef2-dc0d')).toBe('99311ef2-dc0d');
    expect(compactIds('1,2,3')).toBe('1,2,3');
  });
});

import { isRoutineFiling } from '../components/OsintThreatMatrix';

describe('open-source intel', () => {
  it('leaves routine paperwork to the filings page', () => {
    expect(isRoutineFiling('SEC Filing: BANK OF NOVA SCOTIA (BNS) filed Form 424B2')).toBe(true);
    expect(isRoutineFiling('SEC Filing: X Corp filed Form FWP')).toBe(true);
    expect(isRoutineFiling('SEC Filing: Acme filed Form 8-K')).toBe(false);
    expect(isRoutineFiling('Tanker goes dark in the Strait of Hormuz')).toBe(false);
  });
});

import { coverageLine } from '../components/DataChecks';

describe('data checks', () => {
  it('says whether the minute bars add up, in words', () => {
    expect(coverageLine({ available: true, median_coverage: 0.94, ok: true, day: '2026-10-14' })).toBe(
      "Healthy: the typical stock's minute bars add up to 94% of its official daily volume (2026-10-14).",
    );
    expect(coverageLine({ available: true, median_coverage: 0.05, ok: false, day: '2026-10-14' })).toContain('Needs a look');
    expect(coverageLine({ available: false, note: 'Soon.' })).toBe('Soon.');
  });
});

import { healthLine } from '../components/ModelHealth';

describe('model health', () => {
  it('states each horizon in a sentence', () => {
    const base = { chosen_model: 'ridge', supported: true, periods: 20, t: 2.1, expected_ic: 0.067 };
    expect(healthLine({ ...base, mean_ic: 0.071, status: 'ok' })).toBe(
      'Working as promised: ranking score 0.071 live against 0.067 in testing (above zero helps).',
    );
    expect(healthLine({ ...base, mean_ic: -0.04, status: 'broken' })).toContain('Pointing the wrong way');
    expect(healthLine({ ...base, periods: 2, mean_ic: null, t: null, status: 'warming_up' })).toBe(
      'Still collecting results (2 so far). In testing its ranking score was 0.067.',
    );
  });
});

import { changeWords } from '../components/OilStocks';
import { money } from '../components/InsiderIntent';

describe('oil stocks and insider notices', () => {
  it('describes a week in words', () => {
    expect(changeWords({ change: -8557, change_z: -1.74 })).toBe('drew 8.6M barrels, larger than usual');
    expect(changeWords({ change: 3518, change_z: 2.4 })).toBe('built 3.5M barrels, unusually large');
    expect(changeWords({ change: 100, change_z: 0.2 })).toBe('built 0.1M barrels, a normal week');
  });

  it('formats money compactly', () => {
    expect(money(89100)).toBe('$89k');
    expect(money(1_250_000)).toBe('$1.3M');
    expect(money(null)).toBe('—');
  });
});
