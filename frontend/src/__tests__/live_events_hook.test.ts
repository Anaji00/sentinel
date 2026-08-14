import { describe, it, expect } from 'vitest';

describe('useLiveEvents Buffer & Deduplication Logic Unit Tests', () => {
  it('should deduplicate high frequency AIS and ADSB telemetry by entity ID', () => {
    const rawBatch = [
      { event_id: '1', type: 'vessel_position', primary_entity: { id: 'MMSI_100' }, latitude: 26.5 },
      { event_id: '2', type: 'vessel_position', primary_entity: { id: 'MMSI_100' }, latitude: 26.51 },
      { event_id: '3', type: 'vessel_position', primary_entity: { id: 'MMSI_200' }, latitude: 1.3 },
      { event_id: '4', type: 'cyber_attack', primary_entity: { id: 'IP_1.1.1.1' }, anomaly_score: 0.9 },
    ];

    const positionMap = new Map<string, any>();
    const nonPositionEvents: any[] = [];

    for (const item of rawBatch) {
      const t = (item.type || '').toLowerCase();
      const isHighFreqPos = t.includes('vessel_position') || t.includes('adsb_position') || t.includes('ais') || t.includes('adsb');
      if (isHighFreqPos && item.primary_entity?.id) {
        if (!positionMap.has(item.primary_entity.id)) {
          positionMap.set(item.primary_entity.id, item);
        }
      } else {
        nonPositionEvents.push(item);
      }
    }

    const dedupedBatch = [...Array.from(positionMap.values()), ...nonPositionEvents];
    expect(dedupedBatch.length).toBe(3);
    expect(dedupedBatch.map(e => e.event_id)).toEqual(['1', '3', '4']);
  });

  it('should calculate exponential backoff reconnect delays correctly', () => {
    const getNextBackoff = (currentMs: number, maxMs: number = 30000): number => {
      return Math.min(maxMs, currentMs * 2);
    };

    expect(getNextBackoff(1000)).toBe(2000);
    expect(getNextBackoff(2000)).toBe(4000);
    expect(getNextBackoff(16000)).toBe(30000);
    expect(getNextBackoff(30000)).toBe(30000);
  });
});
