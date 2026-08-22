import { create } from 'zustand';

interface TelemetryState {
  isConnected: boolean;
  /**
   * The live feed was refused because the caller is not signed in, as opposed to
   * still connecting. The gateway rejects the WebSocket during the handshake, so
   * the browser only ever reports an abnormal close (1006) and the dashboard sat
   * on "CONNECTING..." indefinitely while the real problem was a missing session.
   */
  authRequired: boolean;
  lastUpdate: number | null;
  setConnected: (status: boolean) => void;
  setAuthRequired: (status: boolean) => void;
  updateTelemetry: () => void;
}

export const useTelemetryStore = create<TelemetryState>((set) => ({
  isConnected: false,
  authRequired: false,
  lastUpdate: null,
  setConnected: (status) => set({ isConnected: status }),
  setAuthRequired: (status) => set({ authRequired: status }),
  updateTelemetry: () => set({ lastUpdate: Date.now() }),
}));
