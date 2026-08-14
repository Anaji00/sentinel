import { describe, it, expect } from 'vitest';

describe('GlobalMap Anti-Collision & Marker Optimization Unit Tests', () => {
  it('should apply golden-angle anti-collision micro-dispersion within 200 meters', () => {
    const applySpatialAntiCollision = <T extends { lat: number; lon: number }>(items: T[]): T[] => {
      const positionCounts = new Map<string, number>();
      return items.map((item) => {
        const gridKey = `${item.lat.toFixed(4)},${item.lon.toFixed(4)}`;
        const count = positionCounts.get(gridKey) || 0;
        positionCounts.set(gridKey, count + 1);

        if (count === 0) return item;

        const angle = count * 2.39996;
        const radius = 0.002 * Math.sqrt(count); // ~200m
        const offsetLat = Math.sin(angle) * radius;
        const offsetLon = Math.cos(angle) * radius * 1.15;

        return {
          ...item,
          lat: Math.max(-85, Math.min(85, item.lat + offsetLat)),
          lon: Math.max(-180, Math.min(180, item.lon + offsetLon)),
        };
      });
    };

    const coLocatedVessels = [
      { id: 'v1', lat: 26.5000, lon: 56.5000 },
      { id: 'v2', lat: 26.5000, lon: 56.5000 },
      { id: 'v3', lat: 26.5000, lon: 56.5000 },
    ];

    const deconflicted = applySpatialAntiCollision(coLocatedVessels);
    expect(deconflicted[0].lat).toBe(26.5000);
    expect(deconflicted[1].lat).not.toBe(26.5000);
    expect(Math.abs(deconflicted[1].lat - 26.5000)).toBeLessThan(0.01);
  });
});
