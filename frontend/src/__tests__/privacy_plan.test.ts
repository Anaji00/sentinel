import { describe, it, expect } from 'vitest';
import { CONCERN_LABEL, planBody } from '../components/PrivacyPlan';

/** The plan request carries only what the person chose, in its body. */
describe('privacy plan request', () => {
  it('sends only chosen, known values', () => {
    expect(planBody('CA', 'iphone', true, false, ['immigration', 'made_up'])).toEqual({
      state: 'CA', phone: 'iphone', has_car: true, has_doorbell_camera: false, concerns: ['immigration'],
    });
    expect(planBody('', 'nokia', false, true, [])).toEqual({
      state: null, phone: null, has_car: false, has_doorbell_camera: true, concerns: [],
    });
  });

  it('labels concerns in plain words', () => {
    expect(Object.keys(CONCERN_LABEL)).toEqual(['immigration', 'reproductive', 'activism_press', 'domestic_violence']);
  });
});
