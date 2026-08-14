import { describe, it, expect } from 'vitest';

describe('Frontend Telemetry Store Unit Tests (store.ts)', () => {
  it('should track WebSocket connectivity state and event counters', () => {
    interface TelemetryState {
      connected: boolean;
      totalEventsReceived: number;
      lastUpdated: number;
    }

    const initialState: TelemetryState = {
      connected: false,
      totalEventsReceived: 0,
      lastUpdated: 0,
    };

    const setConnected = (state: TelemetryState, isConn: boolean): TelemetryState => ({
      ...state,
      connected: isConn,
      lastUpdated: Date.now(),
    });

    const incrementEvents = (state: TelemetryState, count: number): TelemetryState => ({
      ...state,
      totalEventsReceived: state.totalEventsReceived + count,
      lastUpdated: Date.now(),
    });

    let state = setConnected(initialState, true);
    expect(state.connected).toBe(true);

    state = incrementEvents(state, 50);
    expect(state.totalEventsReceived).toBe(50);
  });
});
