import { describe, it, expect, beforeEach } from 'vitest';
import { useTelemetryStore } from '../lib/store';

/**
 * This test imports `store.ts`.
 *
 * The previous version declared its own `TelemetryState` interface and a pair
 * of reducers inside the test body and asserted on those. It was a
 * transcription of the store rather than the store, so it could only fail if
 * the transcription was wrong -- never if the store changed, and never if the
 * store were deleted. It also named fields (`connected`,
 * `totalEventsReceived`) that the real store does not have.
 */
describe('Telemetry store (store.ts)', () => {
  beforeEach(() => {
    useTelemetryStore.setState({ isConnected: false, authRequired: false, lastUpdate: null });
  });

  it('tracks websocket connectivity', () => {
    expect(useTelemetryStore.getState().isConnected).toBe(false);
    useTelemetryStore.getState().setConnected(true);
    expect(useTelemetryStore.getState().isConnected).toBe(true);
  });

  it('distinguishes "refused for want of a session" from "still connecting"', () => {
    // The gateway rejects the WebSocket during the handshake, so the browser
    // only sees an abnormal close. Without this flag the dashboard sits on
    // CONNECTING... forever while the real problem is a missing session.
    expect(useTelemetryStore.getState().authRequired).toBe(false);
    useTelemetryStore.getState().setAuthRequired(true);
    expect(useTelemetryStore.getState().authRequired).toBe(true);
    expect(useTelemetryStore.getState().isConnected).toBe(false);
  });

  it('stamps lastUpdate only once telemetry has actually arrived', () => {
    // null, not 0: "no update yet" and "updated at the epoch" are different
    // statements, and a dashboard rendering an age from 0 shows 56 years.
    expect(useTelemetryStore.getState().lastUpdate).toBeNull();
    useTelemetryStore.getState().updateTelemetry();
    expect(useTelemetryStore.getState().lastUpdate).toBeGreaterThan(0);
  });
});
