import { describe, it, expect } from 'vitest';
import {
  dedupePositionBatch,
  reconnectDelay,
  RECONNECT_INITIAL_MS,
  RECONNECT_MAX_MS,
} from '../lib/useLiveEvents';

/**
 * These tests import `useLiveEvents.ts`.
 *
 * The previous version copied the dedup loop out of the hook and into the test
 * body, character for character, then asserted on the copy -- so it could only
 * ever fail if the transcription was wrong, never if the hook changed. The two
 * pure parts of the hook are now exported and exercised directly.
 */
describe('useLiveEvents buffering', () => {
  it('keeps one position report per entity and every non-position event', () => {
    const batch = [
      { event_id: '1', type: 'vessel_position', primary_entity: { id: 'MMSI_100' }, latitude: 26.5 },
      { event_id: '2', type: 'vessel_position', primary_entity: { id: 'MMSI_100' }, latitude: 26.51 },
      { event_id: '3', type: 'vessel_position', primary_entity: { id: 'MMSI_200' }, latitude: 1.3 },
      { event_id: '4', type: 'cyber_attack', primary_entity: { id: 'IP_1.1.1.1' }, anomaly_score: 0.9 },
    ] as any[];

    const out = dedupePositionBatch(batch);
    const ids = out.map((e) => e.event_id).sort();

    // MMSI_100 collapses to its first report; MMSI_200 and the cyber event survive.
    expect(out).toHaveLength(3);
    expect(ids).toEqual(['1', '3', '4']);
  });

  it('never drops an event that carries no entity id', () => {
    // A position report the platform could not attribute is still an event.
    // Collapsing those together by a missing key would silently lose all but one.
    const batch = [
      { event_id: 'a', type: 'vessel_position', primary_entity: {} },
      { event_id: 'b', type: 'vessel_position', primary_entity: {} },
    ] as any[];
    expect(dedupePositionBatch(batch)).toHaveLength(2);
  });

  it('backs off exponentially and then stops growing', () => {
    expect(reconnectDelay(RECONNECT_INITIAL_MS)).toBe(RECONNECT_INITIAL_MS * 2);
    expect(reconnectDelay(RECONNECT_MAX_MS)).toBe(RECONNECT_MAX_MS);
    expect(reconnectDelay(RECONNECT_MAX_MS * 4)).toBe(RECONNECT_MAX_MS);
  });
});
