import { describe, it, expect, vi } from 'vitest';

vi.mock('leaflet/dist/leaflet.css', () => ({}));

import { intensityColor, KIND_LABEL } from '../components/CivicWatch';

/**
 * The surveillance page speaks plainly about devices and agencies, and its
 * privacy steps are concrete, free, and state-aware where the law differs.
 */
describe('surveillance watch', () => {
  it('names the device kinds in plain words', () => {
    expect(KIND_LABEL.ALPR).toBe('Licence-plate reader');
    expect(KIND_LABEL.gunshot_detector).toBe('Gunshot detector');
  });

  it('colours agencies by how invasive their documented toolkit is', () => {
    expect(intensityColor(0)).toBe('#64748b');
    expect(intensityColor(3)).toBe('#eab308');
    expect(intensityColor(6)).toBe('#f97316');
    expect(intensityColor(12)).toBe('#e11d48');
  });

});
